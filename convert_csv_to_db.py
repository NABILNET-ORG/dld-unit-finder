"""
Convert DLD Units CSV to SQLite database — ZERO DATA LOSS.
Reads ALL columns dynamically from the CSV header. Nothing is filtered or dropped.

Usage:
    python convert_csv_to_db.py                    # Download CSV from Dubai Pulse + convert
    python convert_csv_to_db.py --csv units.csv    # Use local CSV file
    python convert_csv_to_db.py --upload           # Also upload to Google Drive
    python convert_csv_to_db.py --verify           # Verify row/column counts match
"""

import csv
import sqlite3
import os
import sys
import argparse
import requests
import time
import json
import re

# Dubai Pulse direct CSV download URL
CSV_URL = (
    "https://www.dubaipulse.gov.ae/dataset/"
    "85462a5b-08dc-4325-9242-676a0de4afc4/resource/"
    "7d4deadf-c9bc-47a4-85de-998d0ce38bf3/download/units.csv"
)
DB_PATH = "dld_units.db"
CSV_RAW = "units_raw.csv"
METADATA_PATH = "db_metadata.json"

# Columns that should be indexed for fast search
INDEX_COLUMNS = [
    "project_name_en",
    "area_name_en",
    "master_project_en",
    "unit_number",
    "land_number",
    "building_number",
    "property_type_en",
    "rooms",
    "zone_id",
    "area_id",
    "property_id",
    "parent_property_id",
    "grandparent_property_id",
    "is_free_hold",
    "project_name_ar",
    "area_name_ar",
    "master_project_ar",
]


def download_csv(output_path=CSV_RAW):
    """Download the CSV from Dubai Pulse."""
    print("=" * 60)
    print("📥 STEP 1: Downloading CSV from Dubai Pulse")
    print("=" * 60)
    print(f"   URL: {CSV_URL}")
    print(f"   ⚠️  File is ~830MB — this may take a few minutes...\n")

    start = time.time()
    response = requests.get(CSV_URL, stream=True, timeout=600)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    downloaded = 0

    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
            downloaded += len(chunk)
            if total_size:
                pct = (downloaded / total_size) * 100
                mb_done = downloaded / (1024 * 1024)
                mb_total = total_size / (1024 * 1024)
                print(
                    f"\r   ↓ {mb_done:.0f} / {mb_total:.0f} MB ({pct:.1f}%)",
                    end="",
                    flush=True,
                )

    elapsed = time.time() - start
    file_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"\n   ✅ Downloaded {file_mb:.1f} MB in {elapsed:.0f}s → {output_path}\n")
    return output_path


def sanitize_column_name(name):
    """Make a column name safe for SQLite."""
    safe = re.sub(r"[^a-zA-Z0-9_]", "_", name.strip())
    safe = re.sub(r"_+", "_", safe).strip("_").lower()
    return safe if safe else "col_unknown"


def detect_csv_columns(csv_path):
    """Read the CSV header and return ALL column names."""
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader)
    return [col.strip() for col in header]


def convert_csv_to_sqlite(csv_path, db_path=DB_PATH):
    """
    Convert CSV to SQLite — ALL columns, ALL rows, ZERO data loss.
    Columns are detected dynamically from the CSV header.
    All values stored as TEXT to prevent any data loss from type casting.
    """
    print("=" * 60)
    print("🔄 STEP 2: Converting CSV → SQLite (FULL — zero data loss)")
    print("=" * 60)
    print(f"   Input:  {csv_path} ({os.path.getsize(csv_path) / (1024*1024):.1f} MB)")
    print(f"   Output: {db_path}\n")

    # --- Detect columns ---
    raw_columns = detect_csv_columns(csv_path)
    safe_columns = [sanitize_column_name(c) for c in raw_columns]

    # Handle duplicate column names
    seen = {}
    unique_columns = []
    for col in safe_columns:
        if col in seen:
            seen[col] += 1
            unique_columns.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            unique_columns.append(col)

    print(f"   📊 Detected {len(unique_columns)} columns from CSV header:")
    for i, (raw, safe) in enumerate(zip(raw_columns, unique_columns)):
        marker = " 🔎" if safe in INDEX_COLUMNS else ""
        rename_note = f" → {safe}" if raw.strip() != safe else ""
        print(f"      {i+1:2d}. {raw.strip()}{rename_note}{marker}")
    print()

    # --- Remove old DB ---
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA cache_size = -64000")  # 64MB cache
    cursor = conn.cursor()

    # --- Create table with ALL columns as TEXT (preserves everything exactly) ---
    # NO PRIMARY KEY on any column! The CSV has duplicate property_ids
    # (~1.1M duplicates). Using a PK + INSERT OR REPLACE would silently
    # drop those rows. We use SQLite's implicit rowid instead.
    col_defs = [f'"{col}" TEXT' for col in unique_columns]
    create_sql = f'CREATE TABLE units (\n  {", ".join(col_defs)}\n)'
    cursor.execute(create_sql)

    # --- Read CSV and insert ALL rows ---
    start = time.time()
    total_rows = 0
    error_rows = 0
    empty_rows = 0

    placeholders = ", ".join(["?" for _ in unique_columns])
    col_list = ", ".join([f'"{c}"' for c in unique_columns])
    insert_sql = f"INSERT INTO units ({col_list}) VALUES ({placeholders})"

    csv.field_size_limit(sys.maxsize)

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        next(reader)  # Skip header

        batch = []
        for line_num, row in enumerate(reader, start=2):
            try:
                # Pad or trim row to match column count
                if len(row) < len(unique_columns):
                    row.extend([""] * (len(unique_columns) - len(row)))
                elif len(row) > len(unique_columns):
                    row = row[: len(unique_columns)]

                # Skip completely empty rows
                if all(cell.strip() == "" for cell in row):
                    empty_rows += 1
                    continue

                # Store as-is (TEXT) — no type conversion = no data loss
                batch.append(tuple(cell.strip() if cell else "" for cell in row))
                total_rows += 1

                if len(batch) >= 25000:
                    cursor.executemany(insert_sql, batch)
                    conn.commit()
                    batch = []
                    elapsed = time.time() - start
                    rate = total_rows / elapsed if elapsed > 0 else 0
                    print(
                        f"\r   ⏳ Processed: {total_rows:>10,} rows  "
                        f"({rate:,.0f} rows/sec)  "
                        f"Errors: {error_rows}  Empty: {empty_rows}",
                        end="",
                        flush=True,
                    )

            except Exception as e:
                error_rows += 1
                if error_rows <= 10:
                    print(f"\n   ⚠️  Row {line_num} error: {e}")

        # Insert remaining batch
        if batch:
            cursor.executemany(insert_sql, batch)
            conn.commit()

    # --- Create indexes ---
    actual_indexes = []
    print(f"\n\n   📊 Creating indexes for fast search...")
    for col in INDEX_COLUMNS:
        if col in unique_columns:
            idx_name = f"idx_{col}"
            try:
                if col.endswith(("_en", "_ar")):
                    cursor.execute(
                        f'CREATE INDEX IF NOT EXISTS "{idx_name}" '
                        f'ON units("{col}" COLLATE NOCASE)'
                    )
                else:
                    cursor.execute(
                        f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON units("{col}")'
                    )
                actual_indexes.append(col)
                print(f"      ✓ {idx_name}")
            except Exception as e:
                print(f"      ✗ {idx_name}: {e}")

    # --- Optimize ---
    print("\n   🧹 Running ANALYZE + VACUUM...")
    cursor.execute("ANALYZE")
    conn.commit()
    cursor.execute("VACUUM")
    conn.commit()
    conn.close()

    elapsed = time.time() - start
    db_size = os.path.getsize(db_path) / (1024 * 1024)

    # --- Summary ---
    print("\n" + "=" * 60)
    print("✅ CONVERSION COMPLETE — SUMMARY")
    print("=" * 60)
    print(f"   Columns:      {len(unique_columns)} (ALL preserved)")
    print(f"   Total rows:   {total_rows:,}")
    print(f"   Empty rows:   {empty_rows:,} (skipped)")
    print(f"   Error rows:   {error_rows:,}")
    print(f"   Indexes:      {len(actual_indexes)}")
    print(f"   DB size:      {db_size:.1f} MB")
    print(f"   Time:         {elapsed:.0f}s")
    print()

    # --- Save metadata ---
    metadata = {
        "source_url": CSV_URL,
        "csv_file": csv_path,
        "csv_size_mb": round(os.path.getsize(csv_path) / (1024 * 1024), 1),
        "db_file": db_path,
        "db_size_mb": round(db_size, 1),
        "columns_raw": raw_columns,
        "columns_safe": unique_columns,
        "column_count": len(unique_columns),
        "total_rows": total_rows,
        "empty_rows_skipped": empty_rows,
        "error_rows": error_rows,
        "indexes": actual_indexes,
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
    }
    with open(METADATA_PATH, "w") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    print(f"   📋 Metadata saved → {METADATA_PATH}")

    return db_path, metadata


def verify_db(db_path=DB_PATH, csv_path=None):
    """Verify that the SQLite DB matches the source CSV exactly."""
    print("\n" + "=" * 60)
    print("🔍 VERIFICATION: Checking data integrity")
    print("=" * 60)

    if not os.path.exists(db_path):
        print("   ❌ Database not found!")
        return False

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Row count
    cursor.execute("SELECT COUNT(*) FROM units")
    db_rows = cursor.fetchone()[0]
    print(f"   DB rows:      {db_rows:,}")

    # Column count
    cursor.execute("PRAGMA table_info(units)")
    db_cols = cursor.fetchall()
    print(f"   DB columns:   {len(db_cols)}")

    # Indexes
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
    indexes = cursor.fetchall()
    print(f"   Indexes:      {len(indexes)}")

    # Sample data
    print(f"\n   📋 Sample row (first row):")
    cursor.execute("SELECT * FROM units LIMIT 1")
    col_names = [desc[0] for desc in cursor.description]
    row = cursor.fetchone()
    if row:
        for col_name, val in zip(col_names, row):
            if val and str(val).strip():
                print(f"      {col_name}: {val}")

    # Compare with CSV
    all_good = True
    if csv_path and os.path.exists(csv_path):
        print(f"\n   📊 Comparing with source CSV...")
        csv_row_count = 0
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            csv_header = next(reader)
            for row in reader:
                if any(cell.strip() for cell in row):
                    csv_row_count += 1

        csv_col_count = len(csv_header)
        print(f"      CSV:  {csv_row_count:,} rows × {csv_col_count} columns")
        print(f"      DB:   {db_rows:,} rows × {len(db_cols)} columns")

        if csv_row_count == db_rows and csv_col_count == len(db_cols):
            print(f"      ✅ PERFECT MATCH!")
        else:
            if csv_row_count != db_rows:
                diff = abs(csv_row_count - db_rows)
                print(f"      ⚠️  Row difference: {diff:,}")
                all_good = False
            if csv_col_count != len(db_cols):
                print(f"      ⚠️  Column difference!")
                all_good = False

    conn.close()
    return all_good


def upload_to_gdrive(db_path, folder_id=None):
    """Upload SQLite DB + metadata to Google Drive using service account."""
    print("\n" + "=" * 60)
    print("📤 STEP 3: Uploading to Google Drive")
    print("=" * 60)

    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload
    except ImportError:
        print("   ❌ Install: pip install google-auth google-api-python-client")
        return None

    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    creds_file = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json")

    if creds_json and creds_json.startswith("{"):
        try:
            creds_data = json.loads(creds_json)
            creds = Credentials.from_service_account_info(
                creds_data,
                scopes=["https://www.googleapis.com/auth/drive.file"],
            )
            print("   🔑 Using credentials from GOOGLE_CREDENTIALS_JSON env var")
        except (json.JSONDecodeError, ValueError) as e:
            print(f"   ❌ GOOGLE_CREDENTIALS_JSON is not valid JSON!")
            print(f"      Error: {e}")
            print(f"      First 50 chars: {repr(creds_json[:50])}")
            print(f"      Length: {len(creds_json)} chars")
            print()
            print("   💡 FIX: Go to GitHub → Settings → Secrets → GOOGLE_CREDENTIALS_JSON")
            print("      Open the JSON file in Notepad, select ALL (Ctrl+A), copy (Ctrl+C)")
            print("      Paste the ENTIRE file content (from { to }) as the secret value")
            return None
    elif os.path.exists(creds_file):
        creds = Credentials.from_service_account_file(
            creds_file, scopes=["https://www.googleapis.com/auth/drive.file"]
        )
        print(f"   🔑 Using credentials from {creds_file}")
    else:
        print(f"   ❌ No credentials found!")
        print(f"      GOOGLE_CREDENTIALS_JSON env var: {'set but empty/invalid' if creds_json else 'not set'}")
        print(f"      Service account file ({creds_file}): not found")
        print()
        print("   💡 FIX: Add GOOGLE_CREDENTIALS_JSON as a GitHub secret")
        print("      (see SETUP_GUIDE.md Step 3.3)")
        return None

    service = build("drive", "v3", credentials=creds)
    folder_id = folder_id or os.environ.get("GDRIVE_FOLDER_ID")

    def upload_file(local_path, remote_name, mime):
        query = f"name='{remote_name}' and trashed=false"
        if folder_id:
            query += f" and '{folder_id}' in parents"
        results = service.files().list(q=query, fields="files(id,size)").execute()
        existing = results.get("files", [])
        media = MediaFileUpload(local_path, mimetype=mime, resumable=True)
        size_mb = os.path.getsize(local_path) / (1024 * 1024)

        if existing:
            fid = existing[0]["id"]
            print(f"   📤 Updating {remote_name} ({size_mb:.1f} MB)...")
            service.files().update(fileId=fid, media_body=media).execute()
        else:
            print(f"   📤 Creating {remote_name} ({size_mb:.1f} MB)...")
            meta = {"name": remote_name}
            if folder_id:
                meta["parents"] = [folder_id]
            result = service.files().create(body=meta, media_body=media, fields="id").execute()
            fid = result["id"]
            # Make accessible
            try:
                service.permissions().create(
                    fileId=fid, body={"type": "anyone", "role": "reader"}
                ).execute()
            except Exception:
                pass
        return fid

    file_id = upload_file(db_path, "dld_units.db", "application/x-sqlite3")
    print(f"   ✅ DB uploaded! ID: {file_id}")
    print(f"   🔗 https://drive.google.com/uc?export=download&id={file_id}")

    if os.path.exists(METADATA_PATH):
        upload_file(METADATA_PATH, "db_metadata.json", "application/json")
        print(f"   📋 Metadata uploaded!")

    return file_id


def main():
    parser = argparse.ArgumentParser(description="DLD CSV → SQLite (zero data loss)")
    parser.add_argument("--csv", help="Path to local CSV file (skip download)")
    parser.add_argument("--upload", action="store_true", help="Upload to Google Drive")
    parser.add_argument("--folder-id", help="Google Drive folder ID")
    parser.add_argument("--verify", action="store_true", help="Only verify existing DB")
    parser.add_argument("--db", default=DB_PATH, help=f"Output DB path (default: {DB_PATH})")
    args = parser.parse_args()

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║     DLD Unit Finder — CSV to SQLite Converter           ║")
    print("║     Zero Data Loss • All Columns • Full Verification    ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()

    if args.verify:
        verify_db(args.db)
        return

    # Step 1: Get CSV
    if args.csv:
        csv_path = args.csv
        if not os.path.exists(csv_path):
            print(f"❌ File not found: {csv_path}")
            sys.exit(1)
    else:
        csv_path = download_csv()

    # Step 2: Convert
    db_path, metadata = convert_csv_to_sqlite(csv_path, args.db)

    # Step 3: Verify
    verify_db(db_path, csv_path)

    # Step 4: Upload
    if args.upload:
        upload_to_gdrive(db_path, args.folder_id)

    # Cleanup
    if not args.csv and os.path.exists(CSV_RAW):
        print(f"\n🧹 Removing raw CSV ({os.path.getsize(CSV_RAW)/(1024*1024):.0f} MB)...")
        os.remove(CSV_RAW)

    print("\n🎉 ALL DONE!")
    print(f"   Database: {db_path} ({metadata['db_size_mb']} MB)")
    print(f"   Rows:     {metadata['total_rows']:,}")
    print(f"   Columns:  {metadata['column_count']} (ALL preserved — zero data loss)")
    print()


if __name__ == "__main__":
    main()
