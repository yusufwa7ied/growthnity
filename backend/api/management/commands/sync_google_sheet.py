"""
Management command to sync data from Google Sheets to S3, then run pipelines.

Usage:
    # Sync all tabs with default 30-day range
    python manage.py sync_google_sheet --all
    
    # Sync specific tab with custom date range
    python manage.py sync_google_sheet --tab=noon_gcc --start=2025-11-01 --end=2025-11-30
    python manage.py sync_google_sheet --tab=namshi --start=2025-12-01 --end=2025-12-01

The command:
1. Reads raw CSV data from Google Sheet tab
2. Saves it to S3 (overwriting existing file)
3. Runs the appropriate pipeline command
4. Tracks sync status for monitoring

Sheet ID: 16IqdAZKZpCiheH1xCC0K6-FemzAxhq5mMH1LrKPAFXU
Tabs: noon_gcc, noon_egypt, styli, namshi, rdel
"""

from django.core.management.base import BaseCommand
from django.core.management import call_command
from django.utils import timezone
from django.db import models
from datetime import datetime, date, timedelta
import pandas as pd
import requests
from io import StringIO, BytesIO

from api.models import SheetSyncStatus
from api.services.s3_service import s3_service


class Command(BaseCommand):
    help = 'Sync data from Google Sheets to S3, then run pipelines'

    # Google Sheet configuration
    SHEET_ID = "16IqdAZKZpCiheH1xCC0K6-FemzAxhq5mMH1LrKPAFXU"
    
    # Tab configuration: tab_name → (s3_key, pipeline_command)
    TAB_CONFIG = {
        'noon_gcc': ('pipeline-data/noon_gcc.csv', 'run_noon'),
        'noon_egypt': ('pipeline-data/noon_egypt.csv', 'run_noon'),
        'styli': ('pipeline-data/styli.csv', 'run_styli'),
        'namshi': ('pipeline-data/namshi.csv', 'run_nn'),
        'rdel': ('pipeline-data/rdel.csv', 'run_rdel'),
    }

    def add_arguments(self, parser):
        parser.add_argument(
            '--tab',
            type=str,
            choices=list(self.TAB_CONFIG.keys()),
            help='Tab name to sync'
        )
        parser.add_argument(
            '--start',
            type=str,
            help='Start date YYYY-MM-DD (default: 30 days ago)'
        )
        parser.add_argument(
            '--end',
            type=str,
            help='End date YYYY-MM-DD (default: today)'
        )
        parser.add_argument(
            '--all',
            action='store_true',
            help='Sync all configured tabs'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be synced without actually syncing'
        )

    def handle(self, *args, **options):
        tab_name = options.get('tab')
        sync_all = options.get('all', False)
        dry_run = options.get('dry_run', False)

        # Parse date range
        if options.get('start'):
            date_from = datetime.strptime(options['start'], '%Y-%m-%d').date()
        else:
            date_from = date.today() - timedelta(days=30)
        
        if options.get('end'):
            date_to = datetime.strptime(options['end'], '%Y-%m-%d').date()
        else:
            date_to = date.today()

        self.stdout.write("=" * 70)
        self.stdout.write(self.style.SUCCESS("📊 GOOGLE SHEETS → S3 → PIPELINE SYNC"))
        self.stdout.write("=" * 70)
        self.stdout.write(f"📅 Date Range: {date_from} → {date_to}\n")

        if sync_all:
            self.sync_all_tabs(date_from, date_to, dry_run)
        elif tab_name:
            self.sync_single_tab(tab_name, date_from, date_to, dry_run)
        else:
            self.stdout.write(
                self.style.ERROR("❌ Must provide either --all OR --tab")
            )
            return

        self.stdout.write("\n" + "=" * 70)
        self.stdout.write(self.style.SUCCESS("✅ SYNC COMPLETE"))
        self.stdout.write("=" * 70)

    def sync_all_tabs(self, date_from, date_to, dry_run):
        """Sync all configured tabs"""
        self.stdout.write(f"🔄 Syncing all {len(self.TAB_CONFIG)} tabs\n")
        
        for tab_name in self.TAB_CONFIG.keys():
            self.stdout.write(f"\n{'─' * 70}")
            self.stdout.write(f"📋 Processing: {tab_name}")
            self.stdout.write(f"{'─' * 70}")
            
            try:
                self.sync_single_tab(tab_name, date_from, date_to, dry_run)
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"❌ Failed to sync {tab_name}: {str(e)}")
                )
                # Update error status
                status, created = SheetSyncStatus.objects.get_or_create(
                    sheet_id=self.SHEET_ID,
                    tab_name=tab_name,
                    defaults={'consecutive_failures': 0, 'last_error': ''}
                )
                status.last_error = str(e)
                status.consecutive_failures += 1
                status.save()
                continue

    def sync_single_tab(self, tab_name, date_from, date_to, dry_run):
        """Sync a single tab: Sheet → S3 → Pipeline"""
        
        if tab_name not in self.TAB_CONFIG:
            self.stdout.write(
                self.style.ERROR(f"❌ Unknown tab: {tab_name}")
            )
            return
        
        s3_key, pipeline_cmd = self.TAB_CONFIG[tab_name]
        
        # Step 1: Read from Google Sheets
        self.stdout.write(f"\n📥 Step 1: Reading from Google Sheet tab '{tab_name}'...")
        df = self.read_google_sheet(tab_name)
        
        if df is None or df.empty:
            self.stdout.write(self.style.WARNING("⚠️  No data found in sheet"))
            return

        self.stdout.write(f"✅ Retrieved {len(df)} rows, {len(df.columns)} columns")
        
        if dry_run:
            self.stdout.write("\n🔍 DRY RUN - Preview of data:")
            self.stdout.write(f"\nColumns: {list(df.columns)}")
            self.stdout.write(f"\nFirst 3 rows:")
            self.stdout.write(str(df.head(3)))
            self.stdout.write(f"\n\nWould upload to S3: {s3_key}")
            self.stdout.write(f"Would run command: {pipeline_cmd} --start={date_from} --end={date_to}")
            return

        # Step 2: Upload to S3
        self.stdout.write(f"\n📤 Step 2: Uploading to S3: {s3_key}...")
        self.upload_to_s3(df, s3_key)
        self.stdout.write(f"✅ Uploaded successfully")

        # Step 3: Run pipeline
        self.stdout.write(f"\n⚙️  Step 3: Running pipeline: {pipeline_cmd}...")
        try:
            call_command(
                pipeline_cmd,
                start=date_from.strftime('%Y-%m-%d'),
                end=date_to.strftime('%Y-%m-%d')
            )
            self.stdout.write(f"✅ Pipeline completed successfully")
            
            # Update sync status
            SheetSyncStatus.objects.update_or_create(
                sheet_id=self.SHEET_ID,
                tab_name=tab_name,
                defaults={
                    'last_row_processed': len(df),
                    'total_rows_synced': len(df),
                    'last_sync_rows': len(df),
                    'last_sync_time': timezone.now(),
                    'last_error': '',
                    'consecutive_failures': 0
                }
            )
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"❌ Pipeline failed: {str(e)}")
            )
            # Update error status
            status, created = SheetSyncStatus.objects.get_or_create(
                sheet_id=self.SHEET_ID,
                tab_name=tab_name,
                defaults={'consecutive_failures': 0, 'last_error': ''}
            )
            status.last_error = str(e)
            status.consecutive_failures += 1
            status.save()
            raise

    def read_google_sheet(self, tab_name):
        """
        Read data from a Google Sheet tab.
        Uses the Google Sheets export API (works for publicly shared sheets).
        """
        url = f"https://docs.google.com/spreadsheets/d/{self.SHEET_ID}/gviz/tq?tqx=out:csv&sheet={tab_name}"
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Parse CSV
            df = pd.read_csv(StringIO(response.text))
            return df
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400:
                self.stdout.write(
                    self.style.ERROR(
                        f"❌ Tab '{tab_name}' not found or sheet is not publicly accessible"
                    )
                )
            else:
                self.stdout.write(
                    self.style.ERROR(f"❌ HTTP Error: {e}")
                )
            return None
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"❌ Failed to read sheet: {str(e)}")
            )
            return None

    def upload_to_s3(self, df, s3_key):
        """Upload DataFrame to S3 as CSV"""
        try:
            # Convert DataFrame to CSV bytes
            csv_buffer = BytesIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            # Upload to S3
            s3_service.s3_client.put_object(
                Bucket=s3_service.bucket_name,
                Key=s3_key,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
            
        except Exception as e:
            raise Exception(f"Failed to upload to S3: {str(e)}")
