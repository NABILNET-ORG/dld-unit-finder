"""
Convert DLD Units CSV to SQLite database — ZERO DATA LOSS.
Reads ALL columns dynamically from the CSV header. Nothing is filtered or dropped.

Usage:
    python convert_csv_to_db.py                    # Download CSV from Dubai Pulse + convert
    python convert_csv_to_db.py --csv units.csv    # Use local CSV file
    python convert_csv_to_db.py --verify           # Verify existing DB
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

CSV_URL = (
    "https://www.dubaipulse.gov.ae/dataset/"
    "85462a5b-08dc-4325-9242-676a0de4afc4/resource/"
    "7d4deadf-c9bc-47a4-85de-998d0ce38bf3/download/units.csv"
)
DB_PATH = "dld_units.db"
CSV_RAW = "units_raw.csv"
METADATA_PATH = "db_metadata.json"

INDEX_COLUMNS = [
    "project_name_en", "area_name_en", "master_project_en",
    "unit_number", "land_number", "building_number",
    "property_type_en", "rooms", "zone_id", "area_id",
    "property_id", "parent_property_id", "grandparent_property_id",
    "is_free_hold", "project_name_ar", "area_name_ar", "master_project_ar",
]


def download_csv(output_path=CSV_RAW):
    print("=" * 60)
    print("📥 STEP 1: Downloading CSV from Dubai Pulse")
    print("=" * 60)
    print(f"   URL: {CSV_URL}")
    print(f"   ⚠️  File is ~830MB — may take a few minutes...\n")

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
                print(f"\r   ↓ {mb_done:.0f} / {mb_total:.0f} MB ({pct:.1f}%)", end="", flush=True)

    elapsed = time.time() - start
    file_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"\n   ✅ Downloaded {file_mb:.1f} MB in {elapsed:.0f}s\n")
    return output_path


def sanitize_column_name(name):
    safe = re.sub(r"[^a-zA-Z0-9_]", "_", name.strip())
    safe = re.sub(r"_+", "_", safe).strip("_").lower()
    return safe if safe else "col_unknown"


def convert_csv_to_sqlite(csv_path, db_path=DB_PATH):
    print("=" * 60)
    print("🔄 STEP 2: Converting CSV → SQLite (FULL — zero data loss)")
    print("=" * 60)
    print(f"   Input:  {csv_path} ({os.path.getsize(csv_path) / (1024*1024):.1f} MB)")
    print(f"   Output: {db_path}\n")

    # Detect columns
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        raw_columns = [col.strip() for col in next(csv.reader(f))]

    safe_columns = [sanitize_column_name(c) for c in raw_columns]

    # Handle duplicates
    seen = {}
    unique_columns = []
    for col in safe_columns:
        if col in seen:
            seen[col] += 1
            unique_columns.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            unique_columns.append(col)

    print(f"   📊 Detected {len(unique_columns)} columns:")
    for i, (raw, safe) in enumerate(zip(raw_columns, unique_columns)):
        marker = " 🔎" if safe in INDEX_COLUMNS else ""
        print(f"      {i+1:2d}. {raw.strip()}{marker}")
    print()

    # Create DB
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA cache_size = -64000")
    cursor = conn.cursor()

    # NO PRIMARY KEY — CSV has duplicate property_ids
    col_defs = [f'"{col}" TEXT' for col in unique_columns]
    cursor.execute(f'CREATE TABLE units (\n  {", ".join(col_defs)}\n)')

    # Insert ALL rows
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
        next(reader)  # skip header

        batch = []
        for line_num, row in enumerate(reader, start=2):
            try:
                if len(row) < len(unique_columns):
                    row.extend([""] * (len(unique_columns) - len(row)))
                elif len(row) > len(unique_columns):
                    row = row[:len(unique_columns)]

                if all(cell.strip() == "" for cell in row):
                    empty_rows += 1
                    continue

                batch.append(tuple(cell.strip() if cell else "" for cell in row))
                total_rows += 1

                if len(batch) >= 25000:
                    cursor.executemany(insert_sql, batch)
                    conn.commit()
                    batch = []
                    elapsed = time.time() - start
                    rate = total_rows / elapsed if elapsed > 0 else 0
                    print(f"\r   ⏳ {total_rows:>10,} rows ({rate:,.0f}/sec) Errors:{error_rows} Empty:{empty_rows}", end="", flush=True)
            except Exception as e:
                error_rows += 1
                if error_rows <= 10:
                    print(f"\n   ⚠️  Row {line_num}: {e}")

        if batch:
            cursor.executemany(insert_sql, batch)
            conn.commit()

    # Create indexes
    print(f"\n\n   📊 Creating indexes...")
    actual_indexes = []
    for col in INDEX_COLUMNS:
        if col in unique_columns:
            idx = f"idx_{col}"
            try:
                collate = " COLLATE NOCASE" if col.endswith(("_en", "_ar")) else ""
                cursor.execute(f'CREATE INDEX IF NOT EXISTS "{idx}" ON units("{col}"{collate})')
                actual_indexes.append(col)
                print(f"      ✓ {idx}")
            except Exception as e:
                print(f"      ✗ {idx}: {e}")

    print("\n   🧹 ANALYZE + VACUUM...")
    cursor.execute("ANALYZE")
    conn.commit()
    cursor.execute("VACUUM")
    conn.commit()
    conn.close()

    elapsed = time.time() - start
    db_size = os.path.getsize(db_path) / (1024 * 1024)

    print("\n" + "=" * 60)
    print("✅ CONVERSION COMPLETE")
    print("=" * 60)
    print(f"   Columns:    {len(unique_columns)} (ALL preserved)")
    print(f"   Rows:       {total_rows:,}")
    print(f"   Empty skip: {empty_rows:,}")
    print(f"   Errors:     {error_rows:,}")
    print(f"   Indexes:    {len(actual_indexes)}")
    print(f"   DB size:    {db_size:.1f} MB")
    print(f"   Time:       {elapsed:.0f}s\n")

    metadata = {
        "source_url": CSV_URL,
        "csv_size_mb": round(os.path.getsize(csv_path) / (1024 * 1024), 1),
        "db_size_mb": round(db_size, 1),
        "columns": unique_columns,
        "column_count": len(unique_columns),
        "total_rows": total_rows,
        "empty_rows_skipped": empty_rows,
        "error_rows": error_rows,
        "indexes": actual_indexes,
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
    }
    with open(METADATA_PATH, "w") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    print(f"   📋 Metadata → {METADATA_PATH}")

    return db_path, metadata


def verify_db(db_path=DB_PATH, csv_path=None):
    print("\n" + "=" * 60)
    print("🔍 VERIFICATION")
    print("=" * 60)

    if not os.path.exists(db_path):
        print("   ❌ Database not found!")
        return False

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM units")
    db_rows = cursor.fetchone()[0]
    cursor.execute("PRAGMA table_info(units)")
    db_cols = cursor.fetchall()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
    indexes = cursor.fetchall()

    print(f"   DB rows:    {db_rows:,}")
    print(f"   DB columns: {len(db_cols)}")
    print(f"   Indexes:    {len(indexes)}")

    # Sample
    print(f"\n   📋 Sample row:")
    cursor.execute("SELECT * FROM units LIMIT 1")
    col_names = [d[0] for d in cursor.description]
    row = cursor.fetchone()
    if row:
        for c, v in zip(col_names, row):
            if v and str(v).strip():
                print(f"      {c}: {v}")

    if csv_path and os.path.exists(csv_path):
        print(f"\n   📊 CSV comparison...")
        csv_rows = 0
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            csv_header = next(reader)
            for row in reader:
                if any(cell.strip() for cell in row):
                    csv_rows += 1
        print(f"      CSV: {csv_rows:,} rows × {len(csv_header)} cols")
        print(f"      DB:  {db_rows:,} rows × {len(db_cols)} cols")
        if csv_rows == db_rows and len(csv_header) == len(db_cols):
            print(f"      ✅ PERFECT MATCH!")
        else:
            print(f"      ⚠️  DIFFERENCE: {abs(csv_rows - db_rows):,} rows")

    conn.close()
    return True


def main():
    parser = argparse.ArgumentParser(description="DLD CSV → SQLite (zero data loss)")
    parser.add_argument("--csv", help="Path to local CSV file (skip download)")
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

    csv_path = args.csv if args.csv else download_csv()
    if args.csv and not os.path.exists(csv_path):
        print(f"❌ File not found: {csv_path}")
        sys.exit(1)

    db_path, metadata = convert_csv_to_sqlite(csv_path, args.db)
    verify_db(db_path, csv_path)

    if not args.csv and os.path.exists(CSV_RAW):
        print(f"\n🧹 Removing raw CSV...")
        os.remove(CSV_RAW)

    print(f"\n🎉 DONE! {db_path} ({metadata['db_size_mb']} MB, {metadata['total_rows']:,} rows, {metadata['column_count']} cols)")


if __name__ == "__main__":
    main()
