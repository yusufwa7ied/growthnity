# Reef Pipeline - Excel Format Guide

## 📊 Data Format

The Reef pipeline now supports **Excel files with Arabic column names** exported from the Power BI dashboard.

### Required Columns

| Arabic Column | English Meaning | Expected Values |
|--------------|-----------------|-----------------|
| Date - Year | Year | 2025 |
| Date - Quarter | Quarter | Qtr 4 |
| Date - Month | Month | November, December, etc. |
| Date - Day | Day | 1-31 |
| كود الكوبون | Coupon Code | REE129, GNA, CAR5, etc. |
| صافى المبيعات | Net Sales | 123.45, 276.77, etc. |
| تصنيف العميل | Customer Type | **جديد** (FTU) or **مكرر** (RTU) |
| الدول | Country | الامارات, قطر, البحرين, المملكة العربية السعودية, الكويت, عمان |
| رقم الطلب | Order Number | 218267621, 221359561, etc. |
| حالة الطلب | Order Status | تم التوصيل, جاري التوصيل, تم التنفيذ |

### Country Mapping

| Arabic | ISO Code |
|--------|----------|
| الامارات | ARE |
| قطر | QAT |
| البحرين | BHR |
| المملكة العربية السعودية | SAU |
| الكويت | KWT |
| عمان | OMN |

### Customer Type Mapping

| Arabic | Code | Meaning |
|--------|------|---------|
| جديد | FTU | First Time User (New Customer) |
| مكرر | RTU | Repeat User (Returning Customer) |

## 🚀 How to Use

### Step 1: Export Data from Power BI

1. Open Reef Power BI dashboard: https://app.powerbi.com/groups/me/reports/4c156468-2cc8-4ff5-a83e-43efefe1c7f6
2. Export the data table to Excel
3. Save the file (e.g., `reef_november.xlsx`)

### Step 2: Convert Excel to CSV

```bash
# On your Mac
cd /Users/yusuf/Desktop/perf

# Convert Excel to CSV (if needed, or just save as CSV from Excel)
python3 << 'EOF'
import pandas as pd
df = pd.read_excel("reef_november.xlsx")
# Remove summary rows
df = df[df["Date - Year"].astype(str).str.isdigit()]
df.to_csv("reef_november.csv", index=False, encoding='utf-8-sig')
print(f"✅ Converted {len(df)} rows to CSV")
EOF
```

### Step 3: Upload to S3

```bash
cd /Users/yusuf/Desktop/perf/my_project
python upload_to_s3.py reef
```

When prompted, select the CSV file you just created.

### Step 4: Run Pipeline

```bash
ssh -i ~/.ssh/growthnity-key.pem ubuntu@44.210.80.248 \
  "cd /home/ubuntu/growthnity && docker compose exec -T backend python manage.py run_reef --start 2025-11-01 --end 2025-11-30"
```

Replace dates with the actual date range in your data.

## 📋 What the Pipeline Does

1. **Loads** the Excel/CSV from S3
2. **Cleans** data:
   - Removes summary/total rows
   - Builds dates from Date columns
   - Maps Arabic countries to ISO codes
   - Maps Arabic customer types (جديد/مكرر) to FTU/RTU
   - Each row = 1 order
3. **Enriches** data:
   - Matches coupons to partners
   - Applies payout rules
4. **Saves** to `ReefTransaction` table (one row per order)
5. **Aggregates** to `CampaignPerformance` table:
   - Groups by: date, partner, coupon, country
   - Splits FTU and RTU metrics
   - Calculates revenue, payout, profit

## ✅ Verification

After running the pipeline, check:

1. **Django Admin** → Reef Transactions
   - Verify order count matches Excel
   - Check FTU vs RTU distribution
   - Confirm countries are correct (ARE, SAU, etc.)

2. **Campaign Performance** → Filter by Reef
   - Check FTU orders vs RTU orders
   - Verify total sales match
   - Confirm payouts calculated correctly

## 🔄 Schedule (Future)

Once stable, we can automate this:
- Manual weekly: Export → Upload → Run pipeline
- OR contact Reef to provide automated exports
- OR if Power BI workspace access granted, fully automate

## 📝 Example Data

```
Date - Year: 2025
Date - Month: November
Date - Day: 17
كود الكوبون: REE147
صافى المبيعات: 220.34
تصنيف العميل: جديد
الدول: المملكة العربية السعودية
رقم الطلب: 219209646
حالة الطلب: تم التوصيل

→ Transforms to:
created_at: 2025-11-17
coupon: REE147
sales: 220.34
user_type: FTU
country: SAU
order_number: 219209646
```

## 🆘 Troubleshooting

**Error: "Column not found"**
- Check Excel has all required Arabic columns
- Verify column names match exactly (including spaces)

**Error: "Invalid date"**
- Ensure Date - Year, Date - Month, Date - Day are all present
- Check Date - Month is full name (November, not Nov)

**Wrong country codes**
- Check الدول column has exact Arabic names
- Verify country mapping in reef.py

**FTU/RTU not splitting**
- Check تصنيف العميل column has جديد or مكرر
- Verify no typos in Arabic text
