# 🏠 DLD Unit Finder

**Property Finder URL → Unit Number & Full DLD Data** | 100% Free

A web app that extracts unit numbers and full property registration data from Dubai Land Department by matching Property Finder listing URLs against DLD's open dataset of 2.3M+ freehold units.

---

## Features

- Paste a Property Finder link → get unit number, land number, zone, and all registration data
- All 46 DLD columns preserved with zero data loss
- Manual "Update Now" button in sidebar for on-demand refresh
- Auto-updates weekly via GitHub Actions
- Works on any device (mobile, tablet, desktop)
- Completely free — no paid APIs, no subscriptions

## Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────┐
│  Streamlit App   │────▶│ Property     │     │ Dubai Pulse  │
│  (Streamlit      │     │ Finder       │     │ (DLD Open    │
│   Cloud - FREE)  │     │ Scraping     │     │  Data - CSV) │
└────────┬─────────┘     └──────────────┘     └──────┬──────┘
         │                                           │
         │  downloads                       weekly   │
         │  compressed DB                   download  │
    ┌────▼────────┐                          ┌───────▼───────┐
    │   GitHub     │◀─────────────────────────│ GitHub Actions│
    │   Releases   │     gzip + upload        │ (FREE cron)   │
    └──────────────┘                          └───────────────┘
```

## How It Works

1. You paste a Property Finder listing URL
2. The app scrapes property details (project name, area, bedrooms, size)
3. It searches the DLD SQLite database using fuzzy matching
4. Returns matching units with all 46 columns of registration data

## Data Integrity

| Aspect | Detail |
|--------|--------|
| Columns | All 46 from DLD CSV (dynamically detected) |
| Rows | 2,376,922 freehold units |
| Storage | All values stored as TEXT — no type casting, no data loss |
| Verification | Automatic row/column count check after every conversion |
| Indexes | 17 indexes for fast search |
| Compression | ~1.3GB DB → ~200MB gzip for transfer |

## Quick Start

### Prerequisites

- GitHub account
- Streamlit Cloud account (free): https://share.streamlit.io

### Setup

1. **Fork or clone** this repository
2. Go to **Actions** tab → **Weekly DLD Data Update** → **Run workflow**
3. Wait 15–25 minutes for the database to build and upload as a Release
4. If repo is private, create a [Fine-grained token](https://github.com/settings/tokens?type=beta) with **Contents: Read-only** access
5. Deploy on **Streamlit Cloud**:
   - Repository: `your-username/dld-unit-finder`
   - Branch: `main`
   - Main file: `app.py`
   - Secrets:
     ```toml
     GITHUB_REPO = "your-username/dld-unit-finder"
     GITHUB_TOKEN = "github_pat_xxxxx"  # only for private repos
     ```
6. Done. The app auto-updates weekly.

See [SETUP_GUIDE.md](SETUP_GUIDE.md) for detailed step-by-step instructions.

## Local Development

```bash
pip install -r requirements.txt

# Download and convert (first time)
python convert_csv_to_db.py

# Verify data integrity
python convert_csv_to_db.py --verify

# Use a local CSV
python convert_csv_to_db.py --csv /path/to/units.csv

# Run the app
streamlit run app.py
```

## Cost

| Service | Cost |
|---------|------|
| Dubai Pulse Data | Free |
| GitHub Actions | Free (2,000 min/month) |
| GitHub Releases | Free (included storage) |
| Streamlit Cloud | Free (Community plan) |
| **Total** | **$0/month** |

## Project Structure

```
dld-unit-finder/
├── app.py                              # Streamlit web app
├── convert_csv_to_db.py                # CSV → SQLite converter (zero data loss)
├── requirements.txt                    # Python dependencies
├── README.md
├── SETUP_GUIDE.md                      # Detailed setup instructions
├── .gitignore
├── .streamlit/
│   ├── config.toml                     # Streamlit theme
│   └── secrets.toml.example            # Secrets template
└── .github/
    └── workflows/
        └── update_data.yml             # Weekly auto-update via GitHub Actions
```

## Data Source

- **Dubai Land Department** via [Dubai Pulse](https://www.dubaipulse.gov.ae/data/dld-registration/dld_units-open) (Open Data)
- Dataset: `dld_units-open` — all freehold units registered with DLD
- Updated daily by DLD, pulled weekly by this tool
- 46 columns including: unit number, land number, building, project, area, zone, property type, rooms, floor, parking, freehold/leasehold status, registration info, and more

## Limitations

1. **Owner information** (name, phone) is not included in the open dataset — it is protected by law
2. **Off-plan properties** may not yet be registered in DLD
3. **Matching accuracy** depends on project naming consistency between Property Finder and DLD records

## License

For personal use only. Uses publicly available government open data.
git push -u origin main
```

أضف الـ Secrets:
- Settings → Secrets and variables → Actions → New repository secret
- **`GOOGLE_CREDENTIALS_JSON`**: محتوى الـ JSON file كامل
- **`GDRIVE_FOLDER_ID`**: الـ Folder ID

### Step 3: First Run

1. Actions tab → "Weekly DLD Data Update" → **Run workflow**
2. انتظر ~10-15 دقيقة
3. تأكد إنو `dld_units.db` + `db_metadata.json` طلعوا على Google Drive
4. انسخ الـ **File ID** للداتابيس من Google Drive

### Step 4: Deploy Streamlit App

1. [share.streamlit.io](https://share.streamlit.io/) → سجّل بـ GitHub
2. New app → اختار الـ repo → Main file: `app.py`
3. **Advanced Settings** → Secrets:
   ```toml
   GDRIVE_FILE_ID = "YOUR_FILE_ID_HERE"
   ```
4. Deploy! 🚀

### Step 5: استخدام

- افتح الـ app link من أي جهاز
- حطّ لينك Property Finder → اضغط Search
- بدك تحدّث الداتا؟ اضغط **🔄 Update Now** بالـ sidebar
- الداتا بتتحدث كمان تلقائياً كل يوم أحد

---

## 🔧 Local Development

```bash
pip install -r requirements.txt

# Download + convert (first time)
python convert_csv_to_db.py

# Verify data integrity
python convert_csv_to_db.py --verify

# Use local CSV
python convert_csv_to_db.py --csv /path/to/units.csv

# Convert + upload to Google Drive
python convert_csv_to_db.py --upload

# Run app
streamlit run app.py
```

## 🆓 Cost

| Service | Cost |
|---------|------|
| Dubai Pulse Data | FREE |
| GitHub Actions | FREE (2000 min/month) |
| Streamlit Cloud | FREE |
| Google Drive | FREE (15GB) |
| Google Cloud Service Account | FREE |
| **Total** | **$0/month** |

## 📁 Files

```
dld-unit-finder/
├── app.py                          # Streamlit app (search + manual update btn)
├── convert_csv_to_db.py            # CSV → SQLite (zero data loss + verification)
├── requirements.txt                # Dependencies
├── README.md                       # This file
├── .gitignore
├── .streamlit/
│   ├── config.toml                 # Streamlit theme
│   └── secrets.toml.example        # Secrets template
└── .github/
    └── workflows/
        └── update_data.yml         # Weekly auto-update
```

## ⚠️ Limitations

1. **Owner Info**: اسم المالك ورقم تلفونه مش بالـ open data (محمية قانونياً)
2. **Off-plan**: عقارات ما انبنت ممكن ما تكون مسجّلة بعد
3. **Matching**: بيعتمد على اسم المشروع — ممكن يطلع أكتر من نتيجة

## 🔒 For Personal Use Only

Uses publicly available government open data from Dubai Land Department via Dubai Pulse.
