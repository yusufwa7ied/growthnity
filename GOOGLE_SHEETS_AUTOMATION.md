# Google Sheets Automation Guide

## Overview

This system automates data ingestion from a master Google Sheet with multiple tabs (one per advertiser). Each tab maintains the **exact format** of the original CSV download for that advertiser. No format standardization needed - existing pipeline cleaning logic handles everything.

## Architecture

```
Looker Studio (view-only) 
    ↓ Manual copy/paste by team
Google Sheets Master
    ├── Noon_Transactions (Noon CSV format)
    ├── Namshi_Transactions (Namshi CSV format)
    ├── DrNutrition_Transactions (DrNut format)
    ├── Styli_Transactions (Styli format)
    ├── SpringRose_Transactions (SpringRose format)
    ├── Partnerize_Transactions (Partnerize format)
    └── Reef_Transactions (RDEL format)
    ↓ Automated reading every 30 minutes
Django Pipelines (existing cleaning logic)
    ↓ Apply historical rates via resolve_payouts_with_history()
CampaignPerformance Table
    ↓
Dashboard Analytics
```

## Setup Instructions

### 1. Create Master Google Sheet

1. Create a new Google Sheet
2. Create tabs with these **exact names**:
   - `Noon_Transactions`
   - `Namshi_Transactions`
   - `DrNutrition_Transactions`
   - `Styli_Transactions`
   - `SpringRose_Transactions`
   - `Partnerize_Transactions`
   - `Reef_Transactions`

3. **Share the sheet**: Set to "Anyone with the link can view"
   - Click "Share" button
   - Change from "Restricted" to "Anyone with the link"
   - Set permission to "Viewer"
   - Copy the sheet ID from URL: `https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit`

### 2. Team Workflow (Manual Data Entry)

For each advertiser:
1. Open Looker Studio report
2. Select date range and filters
3. Copy data (Ctrl+C / Cmd+C)
4. Go to corresponding Google Sheet tab
5. Paste data starting from row 2 (row 1 = headers)
6. **Important**: Preserve the original CSV format/headers for that advertiser

### 3. Run Manual Sync (Testing)

Test a single advertiser first:

```bash
# Sync Noon data
python manage.py sync_google_sheet \
  --sheet-id=YOUR_SHEET_ID \
  --tab=Noon_Transactions \
  --pipeline=noon \
  --advertiser=Noon

# Dry run to preview data without processing
python manage.py sync_google_sheet \
  --sheet-id=YOUR_SHEET_ID \
  --tab=Noon_Transactions \
  --pipeline=noon \
  --dry-run

# Skip incremental tracking (process all rows)
python manage.py sync_google_sheet \
  --sheet-id=YOUR_SHEET_ID \
  --tab=Noon_Transactions \
  --pipeline=noon \
  --skip-tracking
```

### 4. Sync All Advertisers

Once testing is successful:

```bash
python manage.py sync_google_sheet --sheet-id=YOUR_SHEET_ID --all
```

## Scheduling (Every 30 Minutes)

### Option A: Celery Beat (Recommended for Production)

1. Install Celery + Redis:
```bash
pip install celery redis
```

2. Create `backend/api/tasks.py`:
```python
from celery import shared_task
from django.core.management import call_command

@shared_task
def sync_all_google_sheets():
    """Run Google Sheets sync every 30 minutes"""
    call_command('sync_google_sheet', sheet_id='YOUR_SHEET_ID', all=True)
```

3. Configure in `backend/backend/celery.py`:
```python
from celery import Celery
from celery.schedules import crontab

app = Celery('backend')
app.config_from_object('django.conf:settings', namespace='CELERY')

app.conf.beat_schedule = {
    'sync-google-sheets-every-30-min': {
        'task': 'api.tasks.sync_all_google_sheets',
        'schedule': 1800.0,  # 30 minutes in seconds
    },
}
```

4. Run Celery workers:
```bash
celery -A backend worker -l info
celery -A backend beat -l info
```

### Option B: Cron Job (Simpler Alternative)

1. Edit crontab:
```bash
crontab -e
```

2. Add this line (runs every 30 minutes):
```cron
*/30 * * * * cd /home/ubuntu/growthnity && docker compose exec -T backend python manage.py sync_google_sheet --sheet-id=YOUR_SHEET_ID --all >> /var/log/sheet-sync.log 2>&1
```

## Available Pipelines

| Pipeline Name | Advertiser | CSV Format |
|--------------|------------|------------|
| `noon` | Noon | Noon CSV |
| `namshi` | Namshi | Namshi CSV (similar to Noon) |
| `drnutrition` | Dr Nutrition | DrNut CSV |
| `styli` | Styli | Styli CSV |
| `springrose` | SpringRose | SpringRose CSV |
| `partnerize` | Partnerize | Partnerize CSV |
| `rdel` | Reef | RDEL CSV |

## Incremental Sync Logic

The system tracks the last processed row per tab:

1. **First sync**: Processes all rows, saves row count to `SheetSyncStatus`
2. **Subsequent syncs**: Only processes new rows added since last sync
3. **Force full re-sync**: Use `--skip-tracking` flag

## Monitoring

Check sync status in Django Admin:
- **Sheet Sync Statuses** table shows:
  - Last sync time per tab
  - Total rows processed
  - Number of rows in last sync
  - Any error messages
  - Consecutive failure count

## Error Handling

If a sync fails:
1. Check `SheetSyncStatus` table for error message
2. Verify sheet is publicly accessible
3. Check tab name matches exactly
4. Verify CSV format matches expected pipeline format
5. Review logs: `/var/log/sheet-sync.log` (if using cron)

## Data Flow Details

### For each tab:
1. **Read**: Fetch raw CSV data from Google Sheet tab
2. **Track**: Check `SheetSyncStatus` for last processed row
3. **Filter**: Only process new rows since last sync
4. **Clean**: Pass to advertiser-specific pipeline cleaning logic
5. **Rate Lookup**: Apply `resolve_payouts_with_history()` for historical rates
6. **Calculate**: Compute revenue, payout, profit based on transaction date
7. **Aggregate**: Save to `CampaignPerformance` table
8. **Update**: Save new row count to `SheetSyncStatus`

## Advantages

✅ **No Format Standardization**: Each tab keeps original CSV format  
✅ **Reuses Existing Logic**: All cleaning/transformation code unchanged  
✅ **Incremental Processing**: Only processes new rows (efficient)  
✅ **Historical Rates**: Automatically applies correct rates per transaction date  
✅ **Simple Team Workflow**: Just copy/paste from Looker to Sheet  
✅ **Automated Pipeline**: Runs every 30 minutes without manual intervention  
✅ **Error Tracking**: Monitors failures and consecutive errors  

## Next Steps

1. ✅ Create management command (`sync_google_sheet.py`)
2. ✅ Add tracking model (`SheetSyncStatus`)
3. ⏳ Deploy model migration to server
4. ⏳ Create master Google Sheet with tabs
5. ⏳ Test single advertiser sync
6. ⏳ Set up scheduling (Celery or cron)
7. ⏳ Monitor for 24 hours to ensure stability
8. ⏳ Document for team (Looker → Sheet workflow)

## Questions?

- **Q: What if we add old data?**  
  A: Historical rates will apply correctly based on transaction date
  
- **Q: Can we re-process everything?**  
  A: Yes, use `--skip-tracking` flag to ignore incremental tracking
  
- **Q: What if sheet format changes?**  
  A: Update the advertiser's pipeline cleaning logic (same as CSV changes)
  
- **Q: How to add a new advertiser?**  
  A: Add new tab, update `DEFAULT_CONFIG` in management command, create pipeline processor
