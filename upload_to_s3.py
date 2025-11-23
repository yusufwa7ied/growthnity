#!/usr/bin/env python3
"""
S3 Pipeline CSV Upload Script
==============================

Upload Noon-Namshi or Styli CSV files to S3 and trigger pipeline execution.

Usage:
    python upload_to_s3.py --file /path/to/noon-namshi.csv --pipeline nn --start 2025-11-01 --end 2025-11-23
    python upload_to_s3.py --file /path/to/styli_raw_data.csv --pipeline styli --start 2025-11-01 --end 2025-11-23

Prerequisites:
    - boto3: pip install boto3
    - AWS credentials configured: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env vars
    - API endpoint accessible at BASE_URL
"""

import boto3
import requests
import argparse
import os
import sys
from pathlib import Path
from datetime import datetime

# Configuration
AWS_REGION = "us-east-1"
S3_BUCKET = "growthnity-data"
S3_PIPELINE_FILES = {
    "nn": "pipeline-data/noon-namshi.csv",
    "styli": "pipeline-data/styli_raw_data.csv",
}

# Backend API endpoint (update if different)
API_BASE_URL = "https://your-domain.com/api"  # Change to your AWS server domain
API_TOKEN = None  # Will be set via --token argument


def upload_to_s3(local_file: str, pipeline: str) -> bool:
    """Upload CSV file to S3"""
    if not os.path.exists(local_file):
        print(f"❌ File not found: {local_file}")
        return False
    
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        s3_key = S3_PIPELINE_FILES[pipeline]
        
        print(f"\n📤 Uploading to S3...")
        print(f"   File: {local_file}")
        print(f"   S3 Path: s3://{S3_BUCKET}/{s3_key}")
        
        s3_client.upload_file(local_file, S3_BUCKET, s3_key)
        
        print(f"✅ Successfully uploaded to S3")
        return True
        
    except Exception as e:
        print(f"❌ S3 upload failed: {str(e)}")
        return False


def trigger_pipeline(pipeline: str, start_date: str, end_date: str, api_token: str) -> bool:
    """Trigger pipeline execution via Django API"""
    try:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_token}"
        }
        
        payload = {
            "pipeline": pipeline,
            "start_date": start_date,
            "end_date": end_date
        }
        
        print(f"\n🚀 Triggering pipeline via API...")
        print(f"   Endpoint: POST {API_BASE_URL}/pipelines/trigger/")
        print(f"   Pipeline: {pipeline.upper()}")
        print(f"   Date Range: {start_date} → {end_date}")
        
        response = requests.post(
            f"{API_BASE_URL}/pipelines/trigger/",
            json=payload,
            headers=headers,
            timeout=300  # 5 minute timeout for long-running pipelines
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"\n✅ Pipeline triggered successfully!")
            print(f"   Status: {result.get('status')}")
            print(f"   Message: {result.get('message')}")
            return True
        else:
            print(f"\n❌ Pipeline trigger failed:")
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ API call failed: {str(e)}")
        return False


def validate_dates(start_date: str, end_date: str) -> bool:
    """Validate date format"""
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        if start > end:
            print("❌ start_date must be before end_date")
            return False
        
        return True
    except ValueError:
        print("❌ Dates must be in YYYY-MM-DD format")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Upload CSV to S3 and trigger pipeline execution",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python upload_to_s3.py --file noon-namshi.csv --pipeline nn --start 2025-11-01 --end 2025-11-23 --token YOUR_JWT_TOKEN --url https://your-domain.com/api
  python upload_to_s3.py --file styli_raw_data.csv --pipeline styli --start 2025-11-01 --end 2025-11-23 --token YOUR_JWT_TOKEN --url https://your-domain.com/api
        """
    )
    
    parser.add_argument("--file", required=True, help="Path to CSV file to upload")
    parser.add_argument("--pipeline", required=True, choices=["nn", "styli"], help="Pipeline to trigger (nn or styli)")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--token", required=True, help="JWT authentication token from your dashboard")
    parser.add_argument("--url", default=API_BASE_URL, help=f"API base URL (default: {API_BASE_URL})")
    
    args = parser.parse_args()
    
    # Update global API base URL if provided
    global API_BASE_URL
    API_BASE_URL = args.url
    
    print(f"""
╔════════════════════════════════════════════════════════════╗
║         S3 PIPELINE CSV UPLOAD & TRIGGER                  ║
╚════════════════════════════════════════════════════════════╝
""")
    
    # Validate inputs
    if not validate_dates(args.start, args.end):
        sys.exit(1)
    
    pipeline_name = "Noon-Namshi" if args.pipeline == "nn" else "Styli"
    print(f"📋 Pipeline: {pipeline_name}")
    print(f"📅 Date Range: {args.start} to {args.end}\n")
    
    # Step 1: Upload to S3
    if not upload_to_s3(args.file, args.pipeline):
        sys.exit(1)
    
    # Step 2: Trigger pipeline
    if not trigger_pipeline(args.pipeline, args.start, args.end, args.token):
        sys.exit(1)
    
    print(f"\n{'='*60}")
    print(f"✅ ALL STEPS COMPLETED SUCCESSFULLY")
    print(f"{'='*60}\n")
    print(f"Pipeline will process data and populate the dashboard.")
    print(f"Check your dashboard in a few minutes for updated analytics.\n")


if __name__ == "__main__":
    main()
