# Power BI Reef Automation Setup Guide

## 🎯 Objective
Automate Reef data pipeline by fetching directly from Power BI instead of Google Sheets.

## 📊 Current Power BI Access
- **Email:** hello@growthnity.com
- **Report URL:** https://app.powerbi.com/groups/me/reports/4c156468-2cc8-4ff5-a83e-43efefe1c7f6/9580f36a2579935ad7c4?ctid=767b0a1b-56dc-4ed8-a89f-35fb5f2504ba
- **Tenant ID:** `767b0a1b-56dc-4ed8-a89f-35fb5f2504ba`
- **Report ID:** `4c156468-2cc8-4ff5-a83e-43efefe1c7f6`
- **Page ID:** `9580f36a2579935ad7c4`

## 🔧 Setup Steps

### Step 1: Azure AD App Registration (Required for API Access)

Since you already have access to the Power BI report via email, we need to set up API authentication:

**Option A: Service Principal (Recommended for automation)**

1. Go to Azure Portal: https://portal.azure.com
2. Navigate to: **Azure Active Directory** → **App registrations** → **New registration**
3. Fill in:
   - Name: `Growthnity-PowerBI-API`
   - Supported account types: Single tenant
   - Redirect URI: Leave blank
4. Click **Register**
5. Copy these values:
   - **Application (client) ID**
   - **Directory (tenant) ID** (should be `767b0a1b-56dc-4ed8-a89f-35fb5f2504ba`)
6. Go to **Certificates & secrets** → **New client secret**
7. Create secret and **COPY THE VALUE IMMEDIATELY** (it won't show again)
8. Go to **API permissions** → **Add a permission**
9. Select **Power BI Service** → **Delegated permissions**
10. Add these permissions:
    - `Report.Read.All`
    - `Dataset.Read.All`
11. Click **Grant admin consent** (you need admin access)

**Option B: User Credentials (Simpler but less secure)**

Just use the email + password for `hello@growthnity.com`.

### Step 2: Enable Power BI Service Principal Access

1. Go to Power BI Admin Portal: https://app.powerbi.com/admin-portal
2. Navigate to **Tenant settings**
3. Find **Developer settings** → **Service principals can use Power BI APIs**
4. Enable it and add your app (if using Option A)
5. Go to workspace settings and add the service principal as **Member** or **Contributor**

### Step 3: Find Dataset ID

The report uses a dataset. We need to find the dataset ID:

**Method 1: Via Power BI UI**
1. Open the report: https://app.powerbi.com/groups/me/reports/4c156468-2cc8-4ff5-a83e-43efefe1c7f6
2. Click the **⋯** (More options) → **Settings**
3. Look for the dataset name in the settings
4. Go to workspace → Datasets → Find the dataset → Copy ID from URL

**Method 2: Via API (after setup)**
```bash
# We can use the PowerBIService to fetch it automatically
python manage.py shell
from api.services.powerbi_service import powerbi_service
dataset_id = powerbi_service.get_dataset_id_from_report("me", "4c156468-2cc8-4ff5-a83e-43efefe1c7f6")
print(dataset_id)
```

### Step 4: Add Configuration to Django Settings

Add to `backend/backend/settings.py`:

```python
# Power BI Configuration
POWERBI_TENANT_ID = "767b0a1b-56dc-4ed8-a89f-35fb5f2504ba"

# Option A: Service Principal
POWERBI_CLIENT_ID = "your-client-id-here"
POWERBI_CLIENT_SECRET = "your-client-secret-here"

# Option B: User Credentials
POWERBI_USERNAME = "hello@growthnity.com"
POWERBI_PASSWORD = "your-password-here"  # NEVER commit to git!

# Reef Power BI Report
REEF_POWERBI_REPORT_ID = "4c156468-2cc8-4ff5-a83e-43efefe1c7f6"
REEF_POWERBI_GROUP_ID = "me"  # or actual group/workspace ID
REEF_POWERBI_DATASET_ID = "to-be-determined"  # Get from Step 3
```

**Better: Use Environment Variables**

Add to `.env` file (don't commit):
```bash
POWERBI_TENANT_ID=767b0a1b-56dc-4ed8-a89f-35fb5f2504ba
POWERBI_CLIENT_ID=your-client-id
POWERBI_CLIENT_SECRET=your-secret
REEF_POWERBI_DATASET_ID=your-dataset-id
```

## 🚀 Usage

### Option 1: Fetch Data via DAX Query (Recommended)

Once you know the table structure in the dataset:

```python
from api.services.powerbi_service import powerbi_service

# Example: Fetch all Reef data
dax_query = """
EVALUATE
SUMMARIZECOLUMNS(
    'Reef'[Date],
    'Reef'[Coupon],
    'Reef'[Country],
    "Orders", SUM('Reef'[Orders]),
    "Sales", SUM('Reef'[Sales])
)
"""

group_id = "me"  # or workspace ID
dataset_id = "your-dataset-id"

df = powerbi_service.execute_dax_query(group_id, dataset_id, dax_query)
print(df.head())
```

### Option 2: Export Report to File

```python
from api.services.powerbi_service import powerbi_service

df = powerbi_service.export_report_to_csv(
    group_id="me",
    report_id="4c156468-2cc8-4ff5-a83e-43efefe1c7f6",
    page_name="ReportSection"  # optional
)
```

### Option 3: Discover Dataset Structure

```python
from api.services.powerbi_service import powerbi_service

# List all datasets
datasets = powerbi_service.list_datasets("me")
for ds in datasets:
    print(f"{ds['name']}: {ds['id']}")

# Get tables in dataset
tables_df = powerbi_service.get_tables_in_dataset("me", "your-dataset-id")
print(tables_df)
```

## 📝 Next Steps

1. **Complete Azure AD setup** (Steps 1-2 above)
2. **Get credentials** and add to `.env`
3. **Find dataset ID** (Step 3)
4. **Explore dataset structure** to understand table/column names
5. **Create DAX query** matching current CSV format
6. **Update `reef.py` pipeline** to use Power BI instead of S3 CSV
7. **Test with sample date range**
8. **Schedule automation** (daily/hourly cron job)

## ⚠️ Important Notes

- Power BI API has rate limits (varies by license)
- Service Principal requires Power BI Pro/Premium
- DAX query results limited to ~1M rows (should be fine for Reef)
- Always test queries in Power BI Desktop first
- Store credentials securely (environment variables, not in code)

## 🔍 Troubleshooting

**"Insufficient privileges"**
- Grant admin consent in Azure AD
- Add service principal to workspace
- Ensure tenant settings allow API access

**"Dataset not found"**
- Verify dataset ID
- Check workspace permissions
- Use "me" for personal workspace

**"Authentication failed"**
- Verify tenant ID matches
- Check client secret hasn't expired
- Ensure user has Pro license (for user auth)

## 📚 References

- [Power BI REST API Documentation](https://learn.microsoft.com/en-us/rest/api/power-bi/)
- [Service Principal Setup](https://learn.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal)
- [DAX Query Examples](https://learn.microsoft.com/en-us/dax/)
