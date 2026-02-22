# 🏠 DLD Unit Finder

**Property Finder Link → Unit Number & Full DLD Data** | مجاني 100%

تطبيق ويب بيجيب رقم الوحدة وكل بيانات دائرة الأراضي والأملاك بدبي من لينك Property Finder.

---

## ✨ Features / الميزات

- 🔗 **حطّ لينك Property Finder** → بيطلعلك الـ Unit Number + كل الداتا
- 📊 **46 عمود من DLD** — كلها محفوظة، ولا معلومة بتروح (zero data loss)
- 🔄 **كبسة Update Now** بالـ sidebar — تحديث يدوي بأي وقت
- ⏰ **تحديث تلقائي كل أسبوع** عبر GitHub Actions
- 📱 **بيشتغل على أي جهاز** — موبايل، تابلت، لابتوب
- 🆓 **مجاني 100%** — ولا سنت

---

## 🏗️ Architecture / البنية التقنية

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────┐
│  Streamlit App   │────▶│ Property     │     │ Dubai Pulse  │
│  (FREE)          │     │ Finder       │     │ (DLD Open    │
│                  │     │ Scraping     │     │  Data - CSV) │
└────────┬─────────┘     └──────────────┘     └──────┬──────┘
         │                                           │
         │  reads SQLite                    weekly    │
         │  + manual update btn             download  │
    ┌────▼────────┐                          ┌───────▼───────┐
    │ Google Drive │◀─────────────────────────│ GitHub Actions│
    │ (SQLite DB)  │     auto-upload          │ (FREE cron)   │
    └──────────────┘                          └───────────────┘
```

## 📊 Data Integrity / سلامة البيانات

الـ converter بيحافظ على **كل** البيانات:

| Aspect | Detail |
|--------|--------|
| Columns | **All 46** columns from DLD CSV (dynamically detected) |
| Storage | All values stored as TEXT — no type casting = no data loss |
| Verification | Automatic row/column count verification after conversion |
| Metadata | JSON metadata file with full audit trail |
| Indexes | 17 indexes for fast search |

البيانات يلّي بتنحفظ تشمل:
`unit_number`, `land_number`, `land_sub_number`, `building_number`, `floor`, `rooms`, `actual_area`, `common_area`, `actual_common_area`, `unit_balcony_area`, `unit_parking_number`, `parking_allocation_type`, `property_type`, `property_sub_type`, `project_name` (EN+AR), `master_project` (EN+AR), `area_name` (EN+AR), `zone_id`, `area_id`, `property_id`, `parent_property_id`, `grandparent_property_id`, `is_free_hold`, `is_lease_hold`, `is_registered`, `pre_registration_number`, `munc_number`, `munc_zip_code`, `parcel_id`, `land_type`, `creation_date`, etc.

---

## 📋 Setup Guide / دليل التركيب

### Step 1: Google Cloud Service Account (مرة وحدة بس)

1. روح على [Google Cloud Console](https://console.cloud.google.com/)
2. أنشئ مشروع جديد (أو استخدم موجود)
3. فعّل **Google Drive API**:
   - Navigation Menu → APIs & Services → Library
   - ابحث عن "Google Drive API" → Enable
4. أنشئ Service Account:
   - APIs & Services → Credentials → Create Credentials → Service Account
   - سمّيه `dld-updater`
   - Keys → Add Key → JSON → Download
5. أنشئ مجلد على Google Drive اسمه `DLD-Data`
6. شارك المجلد مع الـ service account email (موجود بالـ JSON)
   - مثلاً: `dld-updater@project-id.iam.gserviceaccount.com`
   - صلاحية **Editor**
7. انسخ الـ **Folder ID** من URL المجلد

### Step 2: GitHub Repository

```bash
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/YOUR_USERNAME/dld-unit-finder.git
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
