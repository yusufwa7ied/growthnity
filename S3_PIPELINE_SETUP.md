# S3 Pipeline Upload & Execution Workflow

## Overview

Your pipeline system now supports automated CSV uploads to S3 with one-click pipeline triggering. This guide explains how to use it.

## Architecture

```
Morning/Afternoon:
1. You upload noon-namshi.csv or styli_raw_data.csv locally
2. Run upload_to_s3.py script → Uploads to S3 + Triggers Pipeline
3. Django backend downloads from S3, runs pipeline, stores data
4. Dashboard updates with new analytics
```

## Setup (One-time)

### 1. Install Local Dependencies
```bash
pip install boto3 requests
```

### 2. Get AWS Credentials
You need AWS credentials with S3 access. Set these as environment variables:
```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
```

Or add to `~/.aws/credentials`:
```
[default]
aws_access_key_id = your-access-key
aws_secret_access_key = your-secret-key
region = us-east-1
```

### 3. Get JWT Authentication Token
You'll need an API token to authenticate. Get it from your backend:
```bash
curl -X POST https://your-domain.com/api/login/ \
  -H "Content-Type: application/json" \
  -d '{"username": "your-username", "password": "your-password"}'
```

This returns:
```json
{
  "access": "eyJ0eXAiOiJKV1QiLCJhbGc...",
  "refresh": "eyJ0eXAiOiJKV1QiLCJhbGc..."
}
```

Copy the `access` token - you'll use this for uploads.

### 4. Update upload_to_s3.py
Edit `upload_to_s3.py` and update these if your server domain is different:
```python
API_BASE_URL = "https://your-domain.com/api"  # Your AWS server
S3_BUCKET = "growthnity-data"  # Should be your bucket name
```

## Daily Workflow

### Scenario 1: Morning Upload (Noon-Namshi)

```bash
python upload_to_s3.py \
  --file /path/to/noon-namshi.csv \
  --pipeline nn \
  --start 2025-11-01 \
  --end 2025-11-23 \
  --token "your-jwt-token-here" \
  --url https://your-domain.com/api
```

**What happens:**
- noon-namshi.csv uploaded to `s3://growthnity-data/pipeline-data/noon-namshi.csv`
- Any old noon-namshi.csv is overwritten
- Pipeline runs automatically
- Data inserted into database
- Dashboard updates

### Scenario 2: Afternoon Upload (Styli)

```bash
python upload_to_s3.py \
  --file /path/to/styli_raw_data.csv \
  --pipeline styli \
  --start 2025-11-01 \
  --end 2025-11-23 \
  --token "your-jwt-token-here" \
  --url https://your-domain.com/api
```

### Re-upload Same Day (e.g., Styli again at 6 PM)

Same command as above - it automatically:
1. ✅ Deletes the old S3 file
2. ✅ Uploads new file with same name
3. ✅ Triggers pipeline with new data

## File Storage

**On S3:**
- Noon-Namshi: `s3://growthnity-data/pipeline-data/noon-namshi.csv`
- Styli: `s3://growthnity-data/pipeline-data/styli_raw_data.csv`

**On Server (Backend):**
- Pipelines read directly from S3 (no local storage needed)
- Data processed and stored in PostgreSQL database

## API Endpoint (Manual Trigger)

If you want to trigger a pipeline without uploading again, use the API directly:

```bash
curl -X POST https://your-domain.com/api/pipelines/trigger/ \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-jwt-token-here" \
  -d '{
    "pipeline": "nn",
    "start_date": "2025-11-01",
    "end_date": "2025-11-23"
  }'
```

Response:
```json
{
  "status": "success",
  "message": "NN pipeline executed successfully",
  "pipeline": "nn",
  "date_range": {
    "start": "2025-11-01",
    "end": "2025-11-23"
  }
}
```

## Permissions

Only **Admin** and **OpsManager** roles can trigger pipelines.

## Environment Setup on Server

Add these to your `.env` file on the AWS server:

```bash
# AWS S3 Configuration
AWS_S3_BUCKET_NAME=growthnity-data
AWS_S3_REGION_NAME=us-east-1
AWS_ACCESS_KEY_ID=your-access-key-id
AWS_SECRET_ACCESS_KEY=your-secret-access-key
```

## Troubleshooting

### "File not found in S3"
- Ensure CSV was uploaded successfully
- Check S3 bucket exists and has the right name
- Verify AWS credentials have S3 permissions

### "Pipeline execution failed"
- Check backend logs: `docker compose logs backend`
- Ensure CSV format matches pipeline expectations
- Verify date range is valid

### "Insufficient permissions"
- Ensure your user role is Admin or OpsManager
- Check JWT token is valid and not expired

### JWT Token Expired
Get a new token:
```bash
curl -X POST https://your-domain.com/api/login/ \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "password"}'
```

## Automation (Optional)

You can schedule uploads with cron jobs:

```bash
# Edit crontab
crontab -e

# Run pipeline every day at 10 AM and 3 PM
0 10 * * * /usr/local/bin/python3 /home/user/upload_to_s3.py --file /data/noon-namshi.csv --pipeline nn --start $(date +\%Y-\%m-\%d) --end $(date +\%Y-\%m-\%d) --token YOUR_TOKEN --url https://your-domain.com/api

0 15 * * * /usr/local/bin/python3 /home/user/upload_to_s3.py --file /data/styli_raw_data.csv --pipeline styli --start $(date +\%Y-\%m-\%d) --end $(date +\%Y-\%m-\%d) --token YOUR_TOKEN --url https://your-domain.com/api
```

## Summary

```
📋 Daily Workflow:
├─ Morning:   Upload Noon-Namshi CSV → Pipeline runs → Data in dashboard
├─ Afternoon: Upload Styli CSV → Pipeline runs → Data in dashboard  
└─ Evening:   Optional re-upload → Old file deleted → Pipeline re-runs

🔄 Features:
✅ Automatic file versioning (overwrites old files)
✅ One-command upload + execute
✅ JWT authentication
✅ S3 storage (scalable, versioned, backed up)
✅ Error handling and logging
✅ Support for date range filtering
```

## Files

- `upload_to_s3.py` - Local upload script
- Backend Endpoint: `/api/pipelines/trigger/` - Django REST API
- S3 Service: `backend/api/services/s3_service.py` - AWS integration
- Pipeline Readers: `backend/api/pipelines/noon_namshi.py`, `styli.py` - Updated to read from S3
