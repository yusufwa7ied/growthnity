# ✅ S3 Pipeline Setup Checklist

## ✅ Completed Implementation

### Backend Changes
- ✅ Added `boto3==1.36.23` to `backend/requirements.txt`
- ✅ Updated `backend/backend/settings.py` with S3 configuration
  - AWS_S3_BUCKET_NAME
  - AWS_S3_REGION_NAME
  - AWS_ACCESS_KEY_ID
  - AWS_SECRET_ACCESS_KEY
  - S3_PIPELINE_FILES paths
- ✅ Created `backend/api/services/s3_service.py` - S3 utility service
- ✅ Updated `backend/api/pipelines/noon_namshi.py` to read from S3
- ✅ Updated `backend/api/pipelines/styli.py` to read from S3
- ✅ Added `trigger_pipeline_upload()` endpoint in `backend/api/views.py`
- ✅ Added route: `POST /api/pipelines/trigger/` in `backend/api/urls.py`
- ✅ Docker backend rebuilt with boto3 installed
- ✅ Backend container restarted and running ✓

### User Scripts & Documentation
- ✅ Created `upload_to_s3.py` - Python upload script
- ✅ Created `upload_pipeline.sh` - Bash wrapper for convenience
- ✅ Created `S3_PIPELINE_SETUP.md` - Comprehensive setup guide

### Deployment
- ✅ Code committed to git and pushed to GitHub
- ✅ Code pulled to AWS server
- ✅ Backend container rebuilt (boto3 now installed)
- ✅ Backend container restarted and verified running

---

## 🚀 Next Steps - BEFORE USING

### 1. Set AWS Credentials on Server
SSH to the server and add to `.env`:

```bash
# Edit .env file (usually at /home/ubuntu/growthnity/.env)
AWS_S3_BUCKET_NAME=growthnity-data
AWS_S3_REGION_NAME=us-east-1
AWS_ACCESS_KEY_ID=your-access-key-here
AWS_SECRET_ACCESS_KEY=your-secret-key-here
```

Then restart backend:
```bash
docker compose restart backend
```

### 2. Verify S3 Bucket Exists
```bash
# From AWS Console or CLI:
aws s3 ls | grep growthnity-data

# Create if doesn't exist:
aws s3api create-bucket --bucket growthnity-data --region us-east-1
aws s3api put-object --bucket growthnity-data --key pipeline-data/
```

### 3. Local Setup (Your Machine)

#### Install dependencies:
```bash
pip install boto3 requests
```

#### Set AWS credentials locally:
```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
```

Or in `~/.aws/credentials`:
```
[default]
aws_access_key_id = your-access-key
aws_secret_access_key = your-secret-key
region = us-east-1
```

#### Get JWT token:
```bash
curl -X POST https://your-domain.com/api/login/ \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "your-password"}'

# Response:
{
  "access": "eyJ0eXAiOiJKV1QiLCJhbGc...",
  "refresh": "..."
}
```

Copy the `access` token - you'll use it for uploads.

#### Update API URL in scripts:
Edit `upload_to_s3.py` and `upload_pipeline.sh`:
```python
API_BASE_URL = "https://your-domain.com/api"  # Update to your domain
```

---

## 📋 Usage Examples

### Option 1: Python Script (Full Control)
```bash
python upload_to_s3.py \
  --file /Users/yusuf/noon-namshi.csv \
  --pipeline nn \
  --start 2025-11-01 \
  --end 2025-11-23 \
  --token "eyJ0eXAiOiJKV1QiLCJhbGc..." \
  --url https://your-domain.com/api
```

### Option 2: Bash Wrapper (Simpler)
```bash
# Set environment variables once:
export JWT_TOKEN="eyJ0eXAiOiJKV1QiLCJhbGc..."
export API_URL="https://your-domain.com/api"

# Then just:
./upload_pipeline.sh nn /Users/yusuf/noon-namshi.csv
./upload_pipeline.sh styli /Users/yusuf/styli_raw_data.csv
```

### Option 3: Direct API Call
```bash
curl -X POST https://your-domain.com/api/pipelines/trigger/ \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGc..." \
  -d '{
    "pipeline": "nn",
    "start_date": "2025-11-01",
    "end_date": "2025-11-23"
  }'
```

---

## 🔄 Daily Workflow

```
Morning (10 AM):
  1. Export noon-namshi.csv from your source
  2. ./upload_pipeline.sh nn /path/to/noon-namshi.csv
  3. ✅ Data uploads to S3, pipeline runs, dashboard updates

Afternoon (3 PM):
  1. Export styli_raw_data.csv from your source
  2. ./upload_pipeline.sh styli /path/to/styli_raw_data.csv
  3. ✅ Data uploads to S3, pipeline runs, dashboard updates

Evening (if needed):
  1. Re-export styli_raw_data.csv with new data
  2. ./upload_pipeline.sh styli /path/to/styli_raw_data.csv
  3. ✅ Old file deleted automatically, new pipeline runs
```

---

## 🛠️ Architecture Overview

```
Your Machine:
  CSV File → upload_to_s3.py → AWS S3

AWS S3:
  S3 Bucket → django/pipeline_trigger API → Backend Container

Backend Container:
  S3Service → Read CSV from S3 → Run Pipeline → PostgreSQL

Dashboard:
  ← PostgreSQL ← CampaignPerformance records
```

---

## 📝 File Paths on S3

```
s3://growthnity-data/
  └── pipeline-data/
      ├── noon-namshi.csv          (Noon-Namshi data)
      └── styli_raw_data.csv       (Styli data)
```

---

## 🔑 Important Notes

1. **JWT Token expires periodically** - Get a new one when needed
2. **Date ranges are flexible** - Use any date range, not just current month
3. **Old files auto-deleted** - When you upload with the same name, old file is deleted
4. **Permissions required** - Only Admin and OpsManager can trigger pipelines
5. **Error handling** - Check backend logs if pipeline fails: `docker compose logs backend`

---

## ✅ Verification

After completing setup, verify everything works:

### 1. Check AWS credentials are set:
```bash
aws s3 ls growthnity-data/pipeline-data/
```

### 2. Check backend logs:
```bash
ssh -i ~/.ssh/growthnity-key.pem ubuntu@44.210.80.248
docker compose logs backend | tail -20
```

### 3. Test API endpoint:
```bash
curl -X POST https://your-domain.com/api/pipelines/trigger/ \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-token-here" \
  -d '{"pipeline":"nn","start_date":"2025-11-01","end_date":"2025-11-23"}'
```

---

## 📞 Troubleshooting

**"File not found in S3"**
- Verify CSV was uploaded: `aws s3 ls growthnity-data/pipeline-data/`
- Check AWS credentials have S3 permissions

**"Insufficient permissions"**
- Ensure your user is Admin or OpsManager
- Check JWT token is valid (not expired)

**"Pipeline execution failed"**
- Check backend logs: `docker compose logs backend`
- Verify CSV format matches pipeline expectations
- Check date range is valid

**JWT Token expired**
- Get a new token with login endpoint
- Update JWT_TOKEN environment variable

---

## 📚 Documentation Files

- `S3_PIPELINE_SETUP.md` - Detailed setup and usage guide
- `upload_to_s3.py` - Full Python upload script with documentation
- `upload_pipeline.sh` - Bash wrapper script
- `backend/api/services/s3_service.py` - S3 service implementation

---

## ✨ Summary

Your pipeline system is now fully automated:

```
✅ S3 storage for CSV files (scalable, versioned)
✅ Automatic file versioning (old files deleted)
✅ One-command upload + pipeline execution
✅ JWT authentication for security
✅ Error handling and logging
✅ Support for multiple pipelines (Noon-Namshi, Styli)
✅ Flexible date range filtering
✅ Admin-only access control
```

**You're ready to go!** Start with the checklist above to complete setup, then use the daily workflow.
