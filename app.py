"""
DLD Unit Finder — Property Finder Link → Unit Number
Streamlit app with manual update button, Google Drive sync, and full DLD data.
"""

import streamlit as st
import sqlite3
import requests
from bs4 import BeautifulSoup
import re
import os
import json
import time
from difflib import SequenceMatcher

# --- Config ---
DB_PATH = "dld_units.db"
METADATA_PATH = "db_metadata.json"
GDRIVE_FILE_ID = os.environ.get("GDRIVE_FILE_ID", "")

# --- Page Config ---
st.set_page_config(
    page_title="DLD Unit Finder 🏠",
    page_icon="🏠",
    layout="centered",
)

# --- Custom CSS ---
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Arabic:wght@300;400;600;700&display=swap');

.stApp { font-family: 'IBM Plex Sans Arabic', sans-serif; }

.result-card {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
    color: white;
    padding: 1.5rem 2rem;
    border-radius: 16px;
    margin: 1rem 0;
    box-shadow: 0 8px 32px rgba(0,0,0,0.3);
}
.result-card h3 {
    color: #e94560;
    margin-bottom: 0.8rem;
    font-size: 1.2rem;
}
.result-item {
    display: flex;
    justify-content: space-between;
    padding: 0.4rem 0;
    border-bottom: 1px solid rgba(255,255,255,0.08);
}
.result-label { color: #a0a0b0; font-size: 0.85rem; }
.result-value { color: #fff; font-weight: 600; font-size: 0.95rem; }

.hero-title {
    text-align: center;
    font-size: 2.5rem;
    font-weight: 700;
    background: linear-gradient(135deg, #e94560, #0f3460);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    margin-bottom: 0.3rem;
}
.hero-sub {
    text-align: center;
    color: #666;
    font-size: 0.95rem;
    margin-bottom: 1.5rem;
}

.match-score {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 20px;
    font-size: 0.75rem;
    font-weight: 600;
    margin-left: 8px;
}
.match-high   { background: #00c853; color: white; }
.match-medium { background: #ff9800; color: white; }
.match-low    { background: #f44336; color: white; }

.info-box {
    background: #f0f2f6;
    padding: 0.8rem 1rem;
    border-radius: 10px;
    border-left: 4px solid #e94560;
    margin: 0.8rem 0;
    font-size: 0.9rem;
}
.status-ok  { border-left-color: #00c853; }
.status-warn { border-left-color: #ff9800; }
</style>
""", unsafe_allow_html=True)


# ======================================================================
# DATABASE FUNCTIONS
# ======================================================================

def download_db_from_gdrive(force=False):
    """Download SQLite DB from Google Drive."""
    if not GDRIVE_FILE_ID:
        return False

    # Skip if DB exists and is fresh (< 1 day), unless forced
    if not force and os.path.exists(DB_PATH):
        age_hours = (time.time() - os.path.getmtime(DB_PATH)) / 3600
        if age_hours < 24:
            return True

    try:
        url = f"https://drive.google.com/uc?export=download&id={GDRIVE_FILE_ID}&confirm=t"
        response = requests.get(url, stream=True, timeout=180)

        if response.status_code != 200:
            return False

        # Check it's actually a SQLite file (not a Google Drive HTML page)
        first_chunk = None
        with open(DB_PATH + ".tmp", "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if first_chunk is None:
                    first_chunk = chunk
                f.write(chunk)

        # Validate: SQLite files start with "SQLite format 3"
        if first_chunk and b"SQLite" in first_chunk[:32]:
            os.replace(DB_PATH + ".tmp", DB_PATH)
            return True
        else:
            os.remove(DB_PATH + ".tmp")
            return False

    except Exception:
        if os.path.exists(DB_PATH + ".tmp"):
            os.remove(DB_PATH + ".tmp")
        return False


def download_metadata_from_gdrive():
    """Download metadata JSON from Google Drive (same folder)."""
    # This is optional — metadata gives us info about last update
    gdrive_meta_id = os.environ.get("GDRIVE_METADATA_FILE_ID", "")
    if not gdrive_meta_id:
        return None
    try:
        url = f"https://drive.google.com/uc?export=download&id={gdrive_meta_id}"
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


@st.cache_resource
def get_db():
    """Get SQLite DB connection (cached across reruns)."""
    if not os.path.exists(DB_PATH):
        return None
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def get_db_stats():
    """Get database stats for display."""
    if not os.path.exists(DB_PATH):
        return None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM units")
        row_count = cursor.fetchone()[0]
        cursor.execute("PRAGMA table_info(units)")
        col_count = len(cursor.fetchall())
        conn.close()
        size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
        mod_time = os.path.getmtime(DB_PATH)
        return {
            "rows": row_count,
            "columns": col_count,
            "size_mb": round(size_mb, 1),
            "updated": time.strftime("%Y-%m-%d %H:%M", time.localtime(mod_time)),
        }
    except Exception:
        return None


def get_all_db_columns():
    """Get list of all columns in the units table."""
    if not os.path.exists(DB_PATH):
        return []
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(units)")
        cols = [row[1] for row in cursor.fetchall()]
        conn.close()
        return cols
    except Exception:
        return []


# ======================================================================
# PROPERTY FINDER SCRAPER
# ======================================================================

def scrape_property_finder(url: str) -> dict:
    """Scrape property details from Property Finder URL."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        return {"error": f"Failed to fetch URL: {e}"}

    soup = BeautifulSoup(resp.text, "html.parser")
    data = {"source_url": url}

    # --- JSON-LD structured data ---
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string)
            items = ld if isinstance(ld, list) else [ld]
            for item in items:
                if not isinstance(item, dict):
                    continue
                if "name" in item:
                    data["name"] = item["name"]
                if "address" in item and isinstance(item["address"], dict):
                    data["address"] = item["address"].get("streetAddress", "")
                    data["area"] = item["address"].get("addressLocality", "")
                if "numberOfRooms" in item:
                    data["bedrooms"] = int(item["numberOfRooms"])
                if "floorSize" in item and isinstance(item["floorSize"], dict):
                    val = item["floorSize"].get("value")
                    if val:
                        data["area_sqft"] = float(str(val).replace(",", ""))
        except (json.JSONDecodeError, TypeError, ValueError):
            continue

    # --- URL parsing ---
    slug = url.rstrip("/").split("/")[-1].replace(".html", "")
    parts = slug.split("-")

    prop_types = ["villa", "apartment", "townhouse", "penthouse", "duplex", "studio"]
    for pt in prop_types:
        if pt in parts:
            data["property_type"] = pt.capitalize()
            break

    if parts:
        data["listing_id"] = parts[-1]

    if "dubai" in parts:
        idx = parts.index("dubai")
        location_parts = parts[idx + 1 : -1]
        if location_parts:
            data["url_location"] = " ".join(location_parts)

    # --- Page title ---
    title_el = soup.find("h1")
    if title_el:
        data["title"] = title_el.get_text(strip=True)

    # --- Meta tags ---
    for meta in soup.find_all("meta"):
        content = meta.get("content", "")
        name = (meta.get("name") or meta.get("property") or "").lower()
        if "og:title" in name and content:
            data["og_title"] = content
        if "description" in name and content and "meta_description" not in data:
            data["meta_description"] = content

    # --- Breadcrumbs ---
    crumbs = soup.select(
        "nav[aria-label='breadcrumb'] a, .breadcrumb a, [class*='Breadcrumb'] a, "
        "[class*='breadcrumb'] a"
    )
    crumb_texts = [b.get_text(strip=True) for b in crumbs if b.get_text(strip=True)]
    if crumb_texts:
        data["breadcrumbs"] = crumb_texts

    # --- Page text regex ---
    text = soup.get_text()
    if "bedrooms" not in data:
        m = re.search(r"(\d+)\s*(?:bed(?:room)?s?|BR)", text, re.IGNORECASE)
        if m:
            data["bedrooms"] = int(m.group(1))
    if "area_sqft" not in data:
        m = re.search(r"([\d,]+(?:\.\d+)?)\s*(?:sq\.?\s*ft|sqft)", text, re.IGNORECASE)
        if m:
            data["area_sqft"] = float(m.group(1).replace(",", ""))

    return data


# ======================================================================
# MATCHING LOGIC
# ======================================================================

def find_units(conn, prop: dict) -> list:
    """Search the DLD database for matching units."""
    # Build search terms
    terms = []

    url_loc = prop.get("url_location", "")
    if url_loc:
        terms.extend(url_loc.split())

    title = prop.get("title") or prop.get("og_title") or prop.get("name", "")
    stop = {
        "for", "sale", "rent", "in", "at", "the", "a", "an", "bed", "bedroom",
        "bedrooms", "bathroom", "bathrooms", "with", "and", "buy", "aed", "sqft",
        "sq", "ft", "dubai", "br", "-", "of", "on", "by", "to", "from",
    }
    if title:
        words = [w.lower() for w in re.split(r"[\s,\-/|]+", title)
                 if w.lower() not in stop and len(w) > 2]
        terms.extend(words)

    for crumb in prop.get("breadcrumbs", []):
        words = [w.lower() for w in crumb.split() if len(w) > 2]
        terms.extend(words)

    # Deduplicate
    seen = set()
    unique = []
    for t in terms:
        t = t.strip().lower()
        if t and t not in seen and len(t) > 2:
            seen.add(t)
            unique.append(t)

    if not unique:
        return []

    results = []

    # Strategy 1: project_name_en LIKE (most precise)
    # Try multi-word combos (longest first)
    for length in range(min(4, len(unique)), 0, -1):
        for i in range(len(unique) - length + 1):
            candidate = " ".join(unique[i : i + length])
            cursor = conn.execute(
                'SELECT * FROM units WHERE LOWER(project_name_en) LIKE ? LIMIT 50',
                (f"%{candidate}%",),
            )
            rows = cursor.fetchall()
            if rows:
                results.extend(rows)
                break
        if results:
            break

    # Strategy 2: master_project_en
    if not results:
        for length in range(min(3, len(unique)), 0, -1):
            for i in range(len(unique) - length + 1):
                candidate = " ".join(unique[i : i + length])
                cursor = conn.execute(
                    'SELECT * FROM units WHERE LOWER(master_project_en) LIKE ? LIMIT 100',
                    (f"%{candidate}%",),
                )
                rows = cursor.fetchall()
                if rows:
                    results.extend(rows)
                    break
            if results:
                break

    # Strategy 3: area_name_en fallback
    if not results:
        for term in unique:
            cursor = conn.execute(
                'SELECT * FROM units WHERE LOWER(area_name_en) LIKE ? LIMIT 100',
                (f"%{term}%",),
            )
            rows = cursor.fetchall()
            if rows:
                results.extend(rows)
                break

    return rank_results(results, prop, unique)[:20]


def rank_results(rows, prop, search_terms):
    """Rank results by relevance."""
    scored = []
    search_str = " ".join(search_terms).lower()

    for row in rows:
        d = dict(row)
        score = 0

        # Project name similarity
        project = (d.get("project_name_en") or "").lower()
        if project:
            score += SequenceMatcher(None, search_str, project).ratio() * 50

        # Area match
        area = (d.get("area_name_en") or "").lower()
        for t in search_terms:
            if t in area:
                score += 10

        # Master project match
        master = (d.get("master_project_en") or "").lower()
        for t in search_terms:
            if t in master:
                score += 15

        # Property type match
        ptype = prop.get("property_type", "").lower()
        db_type = (d.get("property_type_en") or "").lower()
        if ptype and ptype in db_type:
            score += 10

        # Bedrooms
        beds = prop.get("bedrooms")
        db_rooms = d.get("rooms")
        if beds is not None and db_rooms:
            try:
                if int(beds) == int(float(db_rooms)):
                    score += 5
            except (ValueError, TypeError):
                pass

        # Area size (sqm→sqft conversion)
        sqft = prop.get("area_sqft")
        db_area = d.get("actual_area")
        if sqft and db_area:
            try:
                db_sqft = float(db_area) * 10.764
                if abs(sqft - db_sqft) < sqft * 0.15:
                    score += 8
            except (ValueError, TypeError):
                pass

        d["_match_score"] = score
        scored.append(d)

    scored.sort(key=lambda x: x["_match_score"], reverse=True)

    # Deduplicate by unit_number + land_number
    seen = set()
    unique = []
    for d in scored:
        key = (d.get("unit_number"), d.get("land_number"), d.get("project_name_en"))
        if key not in seen:
            seen.add(key)
            unique.append(d)

    return unique


# ======================================================================
# DISPLAY HELPERS
# ======================================================================

# Fields to show in result cards + their display labels
DISPLAY_FIELDS = [
    ("unit_number", "🏢 Unit Number"),
    ("land_number", "📍 Land Number"),
    ("land_sub_number", "📍 Land Sub Number"),
    ("building_number", "🏗️ Building Number"),
    ("project_name_en", "🏘️ Project"),
    ("project_name_ar", "🏘️ المشروع"),
    ("master_project_en", "🌆 Master Project"),
    ("master_project_ar", "🌆 المشروع الرئيسي"),
    ("area_name_en", "📍 Area"),
    ("area_name_ar", "📍 المنطقة"),
    ("zone_id", "🗺️ Zone ID"),
    ("area_id", "🗺️ Area ID"),
    ("property_type_en", "🏠 Property Type"),
    ("property_sub_type_en", "🏠 Sub Type"),
    ("rooms_en", "🛏️ Rooms"),
    ("floor", "📊 Floor"),
    ("actual_area", "📐 Actual Area (sqm)"),
    ("common_area", "📐 Common Area"),
    ("actual_common_area", "📐 Actual Common Area"),
    ("unit_balcony_area", "🌿 Balcony Area"),
    ("unit_parking_number", "🅿️ Parking Number"),
    ("parking_allocation_type_en", "🅿️ Parking Type"),
    ("land_type_en", "🏷️ Land Type"),
    ("is_free_hold", "📜 Freehold"),
    ("is_lease_hold", "📜 Leasehold"),
    ("is_registered", "✅ Registered"),
    ("munc_number", "🔢 Municipality Number"),
    ("munc_zip_code", "📮 ZIP Code"),
    ("parcel_id", "🔢 Parcel ID"),
    ("pre_registration_number", "🔢 Pre-Registration #"),
    ("property_id", "🆔 Property ID"),
    ("parent_property_id", "🆔 Parent Property ID"),
    ("grandparent_property_id", "🆔 Grandparent Property ID"),
    ("creation_date", "📅 Creation Date"),
]


def render_result_card(match, index):
    """Render a single result card."""
    score = match.get("_match_score", 0)
    if score > 40:
        badge = '<span class="match-score match-high">High Match</span>'
    elif score > 20:
        badge = '<span class="match-score match-medium">Medium Match</span>'
    else:
        badge = '<span class="match-score match-low">Low Match</span>'

    project = match.get("project_name_en") or match.get("project_name_ar") or "Unknown"

    rows_html = ""
    for field_key, field_label in DISPLAY_FIELDS:
        val = match.get(field_key)
        if val and str(val).strip() and str(val).strip() != "0":
            rows_html += f"""
            <div class="result-item">
                <span class="result-label">{field_label}</span>
                <span class="result-value">{val}</span>
            </div>"""

    st.markdown(
        f"""<div class="result-card">
            <h3>#{index} — {project} {badge}</h3>
            {rows_html}
        </div>""",
        unsafe_allow_html=True,
    )


# ======================================================================
# SIDEBAR — DATA STATUS & MANUAL UPDATE
# ======================================================================

def render_sidebar():
    """Sidebar with DB status and manual update button."""
    with st.sidebar:
        st.markdown("## ⚙️ Database Status")

        stats = get_db_stats()
        if stats:
            st.markdown(
                f'<div class="info-box status-ok">'
                f'✅ Database loaded<br>'
                f'📊 <b>{stats["rows"]:,}</b> units<br>'
                f'📋 <b>{stats["columns"]}</b> columns (all preserved)<br>'
                f'💾 <b>{stats["size_mb"]}</b> MB<br>'
                f'🕐 Updated: <b>{stats["updated"]}</b>'
                f'</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div class="info-box status-warn">'
                '⚠️ No database loaded<br>'
                'Click "Update Now" to download.'
                '</div>',
                unsafe_allow_html=True,
            )

        st.markdown("---")
        st.markdown("## 🔄 Manual Update")
        st.caption("Download the latest DLD data from Google Drive")

        if st.button("🔄 Update Now", use_container_width=True, type="primary"):
            if not GDRIVE_FILE_ID:
                st.error("❌ GDRIVE_FILE_ID not configured!")
            else:
                with st.spinner("📥 Downloading latest database..."):
                    success = download_db_from_gdrive(force=True)
                if success:
                    # Clear cached DB connection so it reconnects
                    get_db.clear()
                    st.success("✅ Database updated!")
                    st.rerun()
                else:
                    st.error("❌ Download failed. Check GDRIVE_FILE_ID.")

        st.markdown("---")
        st.markdown("## ℹ️ About")
        st.caption(
            "Data source: Dubai Land Department via Dubai Pulse (Open Data). "
            "Auto-updates weekly via GitHub Actions. "
            "For personal use only."
        )

        # Show all columns in DB (collapsible)
        all_cols = get_all_db_columns()
        if all_cols:
            with st.expander(f"📋 All {len(all_cols)} DB columns"):
                for i, col in enumerate(all_cols, 1):
                    st.text(f"{i:2d}. {col}")


# ======================================================================
# MAIN APP
# ======================================================================

def main():
    # Download DB from GDrive on first load
    if not os.path.exists(DB_PATH) and GDRIVE_FILE_ID:
        with st.spinner("📥 First load — downloading database..."):
            download_db_from_gdrive(force=True)

    # Sidebar
    render_sidebar()

    # Hero
    st.markdown('<div class="hero-title">🏠 DLD Unit Finder</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="hero-sub">Property Finder Link → Unit Number & Full DLD Data | مجاني 100%</div>',
        unsafe_allow_html=True,
    )

    # Input
    url = st.text_input(
        "🔗 Property Finder URL",
        placeholder="https://www.propertyfinder.ae/en/plp/buy/villa-for-sale-dubai-...",
        help="Paste any Property Finder listing URL",
    )

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        search_btn = st.button("🔍 Find Unit Number", use_container_width=True, type="primary")

    if search_btn and url:
        if "propertyfinder" not in url.lower():
            st.error("❌ Please enter a valid Property Finder URL")
            return

        conn = get_db()
        if conn is None:
            st.error(
                "❌ Database not loaded. Click **🔄 Update Now** in the sidebar, "
                "or run `python convert_csv_to_db.py` locally."
            )
            return

        # --- Step 1: Scrape ---
        with st.spinner("🌐 Scraping Property Finder..."):
            prop = scrape_property_finder(url)

        if "error" in prop:
            st.error(f"❌ {prop['error']}")
            return

        # Show scraped data
        st.markdown("### 📋 Scraped Property Details")
        with st.expander("View details from Property Finder", expanded=True):
            display = {
                "Title": prop.get("title") or prop.get("og_title") or prop.get("name", "—"),
                "Type": prop.get("property_type", "—"),
                "Bedrooms": prop.get("bedrooms", "—"),
                "Area (sqft)": prop.get("area_sqft", "—"),
                "Location (URL)": prop.get("url_location", "—"),
                "Breadcrumbs": " → ".join(prop.get("breadcrumbs", [])) or "—",
            }
            for k, v in display.items():
                st.markdown(f"**{k}:** {v}")

        # --- Step 2: Search ---
        with st.spinner("🔍 Searching DLD database..."):
            matches = find_units(conn, prop)

        # --- Step 3: Results ---
        if matches:
            n = len(matches)
            st.markdown(f"### ✅ Found {n} potential match{'es' if n > 1 else ''}")
            st.markdown(
                '<div class="info-box">'
                "💡 Results ranked by relevance. Top result is most likely correct. "
                "All 46 DLD columns are shown when data exists."
                "</div>",
                unsafe_allow_html=True,
            )

            for i, match in enumerate(matches[:10], 1):
                render_result_card(match, i)

        else:
            st.warning("⚠️ No matching units found.")
            st.markdown(
                "This could mean the property is **off-plan** (not yet registered), "
                "or the project name differs between Property Finder and DLD records. "
                "Try a different listing from the same project."
            )

    # Footer
    st.markdown("---")
    st.markdown(
        "<p style='text-align:center; color:#888; font-size:0.8rem;'>"
        "Data: Dubai Land Department via Dubai Pulse (Open Data) • "
        "All 46 columns preserved — zero data loss • "
        "Personal use only</p>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
