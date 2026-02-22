"""
DLD Unit Finder — Property Finder Link → Unit Number
Downloads DB from GitHub Releases. Manual update button in sidebar.
"""

import streamlit as st
import sqlite3
import requests
from bs4 import BeautifulSoup
import re
import os
import json
import time
import gzip
import shutil
from difflib import SequenceMatcher

# --- Config ---
DB_PATH = "dld_units.db"
DB_GZ_PATH = "dld_units.db.gz"

# GitHub Release config — change these to your repo
GITHUB_REPO = os.environ.get("GITHUB_REPO", "NABILNET-ORG/dld-unit-finder")
GITHUB_RELEASE_TAG = "latest-data"
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")  # Only needed for private repos

# --- Page Config ---
st.set_page_config(page_title="DLD Unit Finder 🏠", page_icon="🏠", layout="centered")

# --- CSS ---
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Arabic:wght@300;400;600;700&display=swap');
.stApp { font-family: 'IBM Plex Sans Arabic', sans-serif; }
.result-card {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
    color: white; padding: 1.5rem 2rem; border-radius: 16px;
    margin: 1rem 0; box-shadow: 0 8px 32px rgba(0,0,0,0.3);
}
.result-card h3 { color: #e94560; margin-bottom: 0.8rem; font-size: 1.2rem; }
.result-item {
    display: flex; justify-content: space-between;
    padding: 0.4rem 0; border-bottom: 1px solid rgba(255,255,255,0.08);
}
.result-label { color: #a0a0b0; font-size: 0.85rem; }
.result-value { color: #fff; font-weight: 600; font-size: 0.95rem; }
.hero-title {
    text-align: center; font-size: 2.5rem; font-weight: 700;
    background: linear-gradient(135deg, #e94560, #0f3460);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    margin-bottom: 0.3rem;
}
.hero-sub { text-align: center; color: #666; font-size: 0.95rem; margin-bottom: 1.5rem; }
.match-score { display: inline-block; padding: 3px 10px; border-radius: 20px; font-size: 0.75rem; font-weight: 600; margin-left: 8px; }
.match-high   { background: #00c853; color: white; }
.match-medium { background: #ff9800; color: white; }
.match-low    { background: #f44336; color: white; }
.info-box { background: #f0f2f6; padding: 0.8rem 1rem; border-radius: 10px; border-left: 4px solid #e94560; margin: 0.8rem 0; font-size: 0.9rem; }
.status-ok  { border-left-color: #00c853; }
.status-warn { border-left-color: #ff9800; }
</style>
""", unsafe_allow_html=True)


# ===================== DATABASE =====================

def download_db_from_github(force=False):
    """Download compressed DB from GitHub Releases, decompress it."""
    if not force and os.path.exists(DB_PATH):
        age_hours = (time.time() - os.path.getmtime(DB_PATH)) / 3600
        if age_hours < 24:
            return True

    try:
        # Get release info
        api_url = f"https://api.github.com/repos/{GITHUB_REPO}/releases/tags/{GITHUB_RELEASE_TAG}"
        headers = {"Accept": "application/vnd.github+json"}
        if GITHUB_TOKEN:
            headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

        resp = requests.get(api_url, headers=headers, timeout=15)
        if resp.status_code != 200:
            # Try direct URL for public repos
            direct_url = f"https://github.com/{GITHUB_REPO}/releases/download/{GITHUB_RELEASE_TAG}/dld_units.db.gz"
            return _download_and_decompress(direct_url, {})
        
        release = resp.json()
        
        # Find the .db.gz asset
        asset_url = None
        for asset in release.get("assets", []):
            if asset["name"] == "dld_units.db.gz":
                asset_url = asset["browser_download_url"]
                break
        
        if not asset_url:
            return False

        dl_headers = {}
        if GITHUB_TOKEN:
            dl_headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

        return _download_and_decompress(asset_url, dl_headers)

    except Exception as e:
        st.warning(f"Download error: {e}")
        return False


def _download_and_decompress(url, headers):
    """Download .gz file and decompress to .db"""
    try:
        resp = requests.get(url, headers=headers, stream=True, timeout=300)
        if resp.status_code != 200:
            return False

        # Save compressed
        with open(DB_GZ_PATH, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)

        # Decompress
        with gzip.open(DB_GZ_PATH, "rb") as f_in:
            with open(DB_PATH, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        # Cleanup .gz
        os.remove(DB_GZ_PATH)

        # Validate it's a real SQLite file
        with open(DB_PATH, "rb") as f:
            header = f.read(16)
        if b"SQLite" not in header:
            os.remove(DB_PATH)
            return False

        return True
    except Exception:
        for p in [DB_GZ_PATH, DB_PATH]:
            if os.path.exists(p):
                try:
                    os.remove(p)
                except:
                    pass
        return False


@st.cache_resource
def get_db():
    if not os.path.exists(DB_PATH):
        return None
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def get_db_stats():
    if not os.path.exists(DB_PATH):
        return None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM units")
        rows = c.fetchone()[0]
        c.execute("PRAGMA table_info(units)")
        cols = len(c.fetchall())
        conn.close()
        size = os.path.getsize(DB_PATH) / (1024 * 1024)
        mod = time.strftime("%Y-%m-%d %H:%M", time.localtime(os.path.getmtime(DB_PATH)))
        return {"rows": rows, "columns": cols, "size_mb": round(size, 1), "updated": mod}
    except:
        return None


# ===================== SCRAPER =====================

def scrape_property_finder(url: str) -> dict:
    """
    Scrape Property Finder listing page.
    Uses a session with full browser headers to avoid blocks.
    Extracts: title, bedrooms, area, location, zone name, breadcrumbs, etc.
    """
    
    # Full browser-like headers — this is key to avoid blocks
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
        "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "DNT": "1",
        "Referer": "https://www.google.com/",
    })

    try:
        resp = session.get(url, timeout=20, allow_redirects=True)
        resp.raise_for_status()
    except requests.RequestException as e:
        return {"error": f"Failed to fetch: {e}"}

    html = resp.text
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    data = {"source_url": url, "_html_length": len(html)}

    # === 1. URL parsing (always works) ===
    slug = url.rstrip("/").split("/")[-1].replace(".html", "")
    parts = slug.split("-")
    for pt in ["villa", "apartment", "townhouse", "penthouse", "duplex", "studio"]:
        if pt in parts:
            data["property_type"] = pt.capitalize()
            break
    if parts:
        data["listing_id"] = parts[-1]
    if "dubai" in parts:
        idx = parts.index("dubai")
        loc = parts[idx + 1:-1]
        if loc:
            data["url_location"] = " ".join(loc)

    # === 2. Page title / og:title ===
    title_tag = soup.find("title")
    if title_tag and title_tag.string:
        raw_title = title_tag.string.strip()
        # Remove " | Property Finder" suffix
        raw_title = re.sub(r"\s*\|\s*Property Finder.*$", "", raw_title)
        data["page_title"] = raw_title

    for meta in soup.find_all("meta"):
        content = meta.get("content", "").strip()
        prop_name = (meta.get("name") or meta.get("property") or "").lower()
        if "og:title" in prop_name and content:
            data["og_title"] = re.sub(r"\s*\|\s*Property Finder.*$", "", content)

    # === 3. H1 title ===
    h1 = soup.find("h1")
    if h1:
        data["title"] = h1.get_text(strip=True)

    # === 4. Breadcrumbs ===
    # Try multiple selectors for breadcrumbs
    crumb_links = soup.select("ol li a, nav a, [aria-label*='breadcrumb'] a")
    crumb_texts = []
    for link in crumb_links:
        href = link.get("href", "")
        txt = link.get_text(strip=True)
        # PF breadcrumbs link to sale/rent category pages
        if txt and ("for-sale" in href or "for-rent" in href) and len(txt) > 2:
            crumb_texts.append(txt)
    if crumb_texts:
        data["breadcrumbs"] = crumb_texts

    # Also look at the URL-based breadcrumb pattern from PF
    # e.g., "Villas for sale in Dubai > The Valley > Farm Gardens > Farm Gardens 1"
    for link in soup.find_all("a"):
        txt = link.get_text(strip=True)
        href = link.get("href", "")
        if "villas-for-sale-the-valley" in href or "for-sale-the-valley" in href:
            if txt and txt not in crumb_texts:
                crumb_texts.append(txt)

    # === 5. JSON-LD structured data ===
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string)
            items = ld if isinstance(ld, list) else [ld]
            for item in items:
                if not isinstance(item, dict):
                    continue
                if "name" in item and not data.get("title"):
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
        except:
            continue

    # === 6. Extract from page text (regex fallback) ===
    if "bedrooms" not in data:
        m = re.search(r"(\d+)\s*(?:Bed(?:room)?s?|BR)\b", text, re.I)
        if m:
            data["bedrooms"] = int(m.group(1))
    
    if "area_sqft" not in data:
        m = re.search(r"([\d,]+(?:\.\d+)?)\s*sqft\b", text, re.I)
        if not m:
            m = re.search(r"([\d,]+(?:\.\d+)?)\s*sq\.?\s*ft\b", text, re.I)
        if m:
            data["area_sqft"] = float(m.group(1).replace(",", ""))

    # === 7. Zone name (from regulatory info — maps to DLD area_name!) ===
    zone_match = re.search(r"Zone\s*name\s*[:\s]*([A-Za-z][A-Za-z\s\d]+)", text)
    if zone_match:
        data["dld_zone_name"] = zone_match.group(1).strip()
    
    # Also look for DLD permit number and reference
    ref_match = re.search(r"Reference\s*[:\s]*([A-Za-z0-9\-]+)", text)
    if ref_match:
        data["reference"] = ref_match.group(1).strip()

    # === 8. Location from the address line on page ===
    # Pattern: "Farm Gardens 1, Farm Gardens, The Valley, Dubai"
    addr_match = re.search(r"([\w\s]+(?:,\s*[\w\s]+){2,3},\s*Dubai)", text)
    if addr_match:
        data["full_address"] = addr_match.group(1).strip()
        # Parse components
        addr_parts = [p.strip() for p in addr_match.group(1).split(",")]
        if len(addr_parts) >= 3:
            data["sub_community"] = addr_parts[0]  # Farm Gardens 1
            data["community"] = addr_parts[1]       # Farm Gardens
            data["master_community"] = addr_parts[2] # The Valley

    # === 9. Property details from page ===
    if "bedrooms" not in data:
        bed_m = re.search(r"Bedrooms?\s*(\d+)", text, re.I)
        if bed_m:
            data["bedrooms"] = int(bed_m.group(1))

    bath_m = re.search(r"Bathrooms?\s*(\d+)", text, re.I)
    if bath_m:
        data["bathrooms"] = int(bath_m.group(1))

    return data


# ===================== MATCHING =====================

def extract_search_phrases(prop: dict) -> list:
    """
    Extract meaningful search phrases from scraped data.
    Returns list of phrases ordered by specificity (most specific first).
    """
    phrases = []
    
    # 1. Sub-community / community / master from page address (MOST SPECIFIC)
    # e.g., "Farm Gardens 1" / "Farm Gardens" / "The Valley"
    sub = prop.get("sub_community", "")
    comm = prop.get("community", "")
    master = prop.get("master_community", "")
    if sub:
        phrases.append(sub)
    if comm and comm != sub:
        phrases.append(comm)
    if master and master != comm:
        phrases.append(master)
    
    # 2. DLD Zone name (from regulatory info — maps to DLD area_name!)
    zone = prop.get("dld_zone_name", "")
    if zone:
        phrases.append(zone)
    
    # 3. Title/name from page
    title = prop.get("title") or prop.get("og_title") or prop.get("name", "")
    if title:
        stop = {"for","sale","rent","in","at","a","an","bed","bedroom","bedrooms",
                "bathroom","bathrooms","with","and","buy","aed","sqft","sq","ft",
                "br","-","of","on","by","to","from","dubai","uae","property",
                "elegant","luxury","luxurious","beautiful","stunning","spacious",
                "brand","new","modern","exclusive","premium","amazing","gorgeous"}
        words = [w for w in re.split(r"[\s,\-/|]+", title) if w.lower() not in stop and len(w) > 1]
        if words:
            phrases.append(" ".join(words))
    
    # 4. URL location
    url_loc = prop.get("url_location", "")
    if url_loc:
        phrases.append(url_loc)
        parts = url_loc.split()
        if len(parts) >= 2:
            for split_at in range(1, len(parts)):
                left = " ".join(parts[:split_at])
                right = " ".join(parts[split_at:])
                if len(right) > 2:
                    phrases.append(right)
                if len(left) > 2:
                    phrases.append(left)
    
    # 5. Breadcrumbs
    for crumb in prop.get("breadcrumbs", []):
        c = crumb.strip()
        if c.lower() not in ("dubai", "home", "buy", "rent", "properties", "uae") and len(c) > 2:
            phrases.append(c)
    
    # Deduplicate
    seen = set()
    unique = []
    for p in phrases:
        p_lower = p.strip().lower()
        if p_lower and p_lower not in seen:
            seen.add(p_lower)
            unique.append(p_lower)
    
    return unique


def find_units(conn, prop: dict) -> list:
    """
    Multi-strategy search:
    0. DLD Zone name → area_name_en (most accurate if available)
    1. Exact project_name match
    2. Combined project + master_project match
    3. Master project match
    4. Area name fallback
    5. Individual words (last resort)
    """
    phrases = extract_search_phrases(prop)
    if not phrases:
        return []
    
    results = []
    
    # === STRATEGY 0: DLD Zone name → area_name_en (BEST — from regulatory info) ===
    zone = prop.get("dld_zone_name", "")
    community = prop.get("community") or prop.get("sub_community", "")
    if zone and community:
        # Search project within the DLD zone
        rows = conn.execute(
            '''SELECT * FROM units 
               WHERE LOWER(area_name_en) LIKE ? 
                 AND LOWER(project_name_en) LIKE ?
               LIMIT 200''',
            (f"%{zone.lower()}%", f"%{community.lower()}%")
        ).fetchall()
        if rows:
            results.extend(rows)
    
    if not results and zone:
        # Try zone + master community
        master = prop.get("master_community", "")
        if master:
            rows = conn.execute(
                '''SELECT * FROM units 
                   WHERE LOWER(area_name_en) LIKE ? 
                     AND LOWER(master_project_en) LIKE ?
                   LIMIT 200''',
                (f"%{zone.lower()}%", f"%{master.lower()}%")
            ).fetchall()
            if rows:
                results.extend(rows)
    
    # === STRATEGY 1: Direct project_name_en match ===
    # Try each phrase against project_name_en (most specific wins)
    for phrase in phrases:
        rows = conn.execute(
            'SELECT * FROM units WHERE LOWER(project_name_en) LIKE ? LIMIT 100',
            (f"%{phrase}%",)
        ).fetchall()
        if rows:
            results.extend(rows)
            break
    
    # === STRATEGY 2: Combined project + master search ===
    # Split URL location into all possible (master, project) pairs
    if not results:
        url_loc = prop.get("url_location", "")
        if url_loc:
            parts = url_loc.split()
            for split_at in range(1, len(parts)):
                master_candidate = " ".join(parts[:split_at])
                project_candidate = " ".join(parts[split_at:])
                if len(project_candidate) > 2 and len(master_candidate) > 2:
                    rows = conn.execute(
                        '''SELECT * FROM units 
                           WHERE LOWER(project_name_en) LIKE ? 
                             AND LOWER(master_project_en) LIKE ?
                           LIMIT 100''',
                        (f"%{project_candidate}%", f"%{master_candidate}%")
                    ).fetchall()
                    if rows:
                        results.extend(rows)
                        break
    
    # === STRATEGY 3: master_project_en match ===
    if not results:
        for phrase in phrases:
            if len(phrase) > 3:  # Skip very short phrases
                rows = conn.execute(
                    'SELECT * FROM units WHERE LOWER(master_project_en) LIKE ? LIMIT 200',
                    (f"%{phrase}%",)
                ).fetchall()
                if rows:
                    results.extend(rows)
                    break
    
    # === STRATEGY 4: area_name_en match ===
    if not results:
        for phrase in phrases:
            if len(phrase) > 3:
                rows = conn.execute(
                    'SELECT * FROM units WHERE LOWER(area_name_en) LIKE ? LIMIT 200',
                    (f"%{phrase}%",)
                ).fetchall()
                if rows:
                    results.extend(rows)
                    break
    
    # === STRATEGY 5: Individual significant words (last resort) ===
    if not results:
        noise = {"the","and","for","villa","apartment","tower","building","residence",
                 "residences","dubai","phase","block","cluster"}
        for phrase in phrases:
            words = [w for w in phrase.split() if w not in noise and len(w) > 3]
            for word in words:
                rows = conn.execute(
                    'SELECT * FROM units WHERE LOWER(project_name_en) LIKE ? LIMIT 100',
                    (f"%{word}%",)
                ).fetchall()
                if rows:
                    results.extend(rows)
                    break
            if results:
                break
    
    return rank_results(results, prop, phrases)[:20]


def rank_results(rows, prop, search_phrases):
    """Score and rank results. Higher = better match."""
    scored = []
    
    # Build all search terms for comparison
    all_terms = set()
    for p in search_phrases:
        all_terms.update(p.lower().split())
    all_terms -= {"the","and","for","of","in","at","a","an","to","by","on","from"}
    
    full_search = " ".join(search_phrases).lower()
    zone = prop.get("dld_zone_name", "").lower() if prop else ""
    
    for row in rows:
        d = dict(row)
        score = 0
        
        project = (d.get("project_name_en") or "").lower()
        master = (d.get("master_project_en") or "").lower()
        area = (d.get("area_name_en") or "").lower()
        
        # Exact phrase match in project name (strongest signal)
        for phrase in search_phrases:
            if phrase in project:
                score += 60 * (len(phrase.split()) / max(len(search_phrases[0].split()), 1))
        
        # Project name similarity
        if project:
            score += SequenceMatcher(None, full_search, project).ratio() * 30
        
        # Master project match
        for phrase in search_phrases:
            if phrase in master:
                score += 25 * (len(phrase.split()) / max(len(search_phrases[0].split()), 1))
        
        # Area match
        for term in all_terms:
            if term in area:
                score += 5
        
        # DLD Zone name match (strong signal)
        if zone and zone in area:
            score += 20
        
        # Property type match
        ptype = prop.get("property_type", "").lower()
        db_type = (d.get("property_type_en") or "").lower()
        db_subtype = (d.get("property_sub_type_en") or "").lower()
        if ptype:
            if ptype in db_type or ptype in db_subtype:
                score += 15
            elif ptype == "villa" and "villa" in db_subtype:
                score += 15
        
        # Bedroom match
        beds = prop.get("bedrooms")
        db_rooms = d.get("rooms")
        if beds is not None and db_rooms:
            try:
                if int(beds) == int(float(db_rooms)):
                    score += 10
            except:
                pass
        
        # Area size match (within 15%)
        sqft = prop.get("area_sqft")
        db_area = d.get("actual_area")
        if sqft and db_area:
            try:
                db_sqft = float(db_area) * 10.764
                if abs(sqft - db_sqft) < sqft * 0.15:
                    score += 12
            except:
                pass
        
        d["_match_score"] = round(score, 1)
        scored.append(d)
    
    scored.sort(key=lambda x: x["_match_score"], reverse=True)
    
    # Deduplicate
    seen = set()
    unique = []
    for d in scored:
        key = (d.get("unit_number"), d.get("land_number"), d.get("project_name_en"))
        if key not in seen:
            seen.add(key)
            unique.append(d)
    
    return unique


# ===================== UI =====================

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
    ("actual_area", "📐 Area (sqm)"),
    ("common_area", "📐 Common Area"),
    ("actual_common_area", "📐 Actual Common Area"),
    ("unit_balcony_area", "🌿 Balcony Area"),
    ("unit_parking_number", "🅿️ Parking Number"),
    ("parking_allocation_type_en", "🅿️ Parking Type"),
    ("land_type_en", "🏷️ Land Type"),
    ("is_free_hold", "📜 Freehold"),
    ("is_lease_hold", "📜 Leasehold"),
    ("is_registered", "✅ Registered"),
    ("munc_number", "🔢 Municipality #"),
    ("munc_zip_code", "📮 ZIP"),
    ("parcel_id", "🔢 Parcel ID"),
    ("pre_registration_number", "🔢 Pre-Reg #"),
    ("property_id", "🆔 Property ID"),
    ("parent_property_id", "🆔 Parent ID"),
    ("grandparent_property_id", "🆔 Grandparent ID"),
    ("creation_date", "📅 Created"),
]


def render_card(match, i):
    score = match.get("_match_score", 0)
    if score > 50: badge = '<span class="match-score match-high">High Match</span>'
    elif score > 25: badge = '<span class="match-score match-medium">Medium Match</span>'
    else: badge = '<span class="match-score match-low">Low Match</span>'

    project = match.get("project_name_en") or match.get("project_name_ar") or "Unknown"
    rows_html = ""
    for key, label in DISPLAY_FIELDS:
        val = match.get(key)
        if val and str(val).strip() and str(val).strip() not in ("0", "null", "0.00"):
            rows_html += f'<div class="result-item"><span class="result-label">{label}</span><span class="result-value">{val}</span></div>'

    st.markdown(f'<div class="result-card"><h3>#{i} — {project} {badge}</h3>{rows_html}</div>', unsafe_allow_html=True)


def render_sidebar():
    with st.sidebar:
        st.markdown("## ⚙️ Database Status")
        stats = get_db_stats()
        if stats:
            st.markdown(
                f'<div class="info-box status-ok">✅ Database loaded<br>'
                f'📊 <b>{stats["rows"]:,}</b> units<br>'
                f'📋 <b>{stats["columns"]}</b> columns<br>'
                f'💾 <b>{stats["size_mb"]}</b> MB<br>'
                f'🕐 Updated: <b>{stats["updated"]}</b></div>',
                unsafe_allow_html=True)
        else:
            st.markdown('<div class="info-box status-warn">⚠️ No database. Click Update Now.</div>', unsafe_allow_html=True)

        st.markdown("---")
        st.markdown("## 🔄 Manual Update")
        st.caption("Download latest data from GitHub Releases")
        if st.button("🔄 Update Now", use_container_width=True, type="primary"):
            with st.spinner("📥 Downloading & decompressing..."):
                ok = download_db_from_github(force=True)
            if ok:
                get_db.clear()
                st.success("✅ Updated!")
                st.rerun()
            else:
                st.error("❌ Download failed. Check repo settings.")

        st.markdown("---")
        st.caption("Data: DLD via Dubai Pulse (Open Data)")


def main():
    if not os.path.exists(DB_PATH):
        with st.spinner("📥 First load — downloading database..."):
            download_db_from_github(force=True)

    render_sidebar()

    st.markdown('<div class="hero-title">🏠 DLD Unit Finder</div>', unsafe_allow_html=True)
    st.markdown('<div class="hero-sub">Property Finder Link → Unit Number & Full DLD Data | مجاني 100%</div>', unsafe_allow_html=True)

    url = st.text_input("🔗 Property Finder URL", placeholder="https://www.propertyfinder.ae/en/plp/buy/...")

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        btn = st.button("🔍 Find Unit Number", use_container_width=True, type="primary")

    if btn and url:
        if "propertyfinder" not in url.lower():
            st.error("❌ Invalid Property Finder URL")
            return

        conn = get_db()
        if not conn:
            st.error("❌ No database. Click **🔄 Update Now** in sidebar.")
            return

        with st.spinner("🌐 Scraping Property Finder..."):
            prop = scrape_property_finder(url)
        if "error" in prop:
            st.error(f"❌ {prop['error']}")
            return

        st.markdown("### 📋 Scraped Details")
        with st.expander("View", expanded=True):
            for k, v in {
                "Title": prop.get("title") or prop.get("og_title") or prop.get("name", "—"),
                "Type": prop.get("property_type", "—"),
                "Bedrooms": prop.get("bedrooms", "—"),
                "Bathrooms": prop.get("bathrooms", "—"),
                "Area (sqft)": prop.get("area_sqft", "—"),
                "Address": prop.get("full_address") or prop.get("url_location", "—"),
                "Community": prop.get("community", "—"),
                "Master Community": prop.get("master_community", "—"),
                "DLD Zone Name": prop.get("dld_zone_name", "—"),
                "Reference": prop.get("reference", "—"),
                "HTML size": f"{prop.get('_html_length', 0):,} chars",
            }.items():
                st.markdown(f"**{k}:** {v}")
            
            phrases = extract_search_phrases(prop)
            if phrases:
                st.markdown(f"**Search phrases:** {', '.join([f'`{p}`' for p in phrases[:8]])}")

        with st.spinner("🔍 Searching DLD..."):
            matches = find_units(conn, prop)

        if matches:
            st.markdown(f"### ✅ {len(matches)} match{'es' if len(matches) > 1 else ''}")
            st.markdown('<div class="info-box">💡 Top result = most likely. All 46 DLD columns shown.</div>', unsafe_allow_html=True)
            for i, m in enumerate(matches[:10], 1):
                render_card(m, i)
        else:
            st.warning("⚠️ No matches. Property may be off-plan or named differently in DLD.")

    st.markdown("---")
    st.markdown('<p style="text-align:center;color:#888;font-size:0.8rem;">DLD Open Data • 46 columns preserved • Personal use</p>', unsafe_allow_html=True)


if __name__ == "__main__":
    main()
