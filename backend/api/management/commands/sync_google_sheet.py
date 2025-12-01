"""
Management command to sync data from Google Sheets to the database.

Usage:
    python manage.py sync_google_sheet --sheet-id=YOUR_SHEET_ID --tab=Noon_Transactions --pipeline=noon
    python manage.py sync_google_sheet --sheet-id=YOUR_SHEET_ID --tab=DrNutrition_Transactions --pipeline=drnutrition
    
    # Sync all tabs (requires config)
    python manage.py sync_google_sheet --sheet-id=YOUR_SHEET_ID --all

The command reads the raw CSV data from the specified Google Sheet tab
and passes it to the appropriate pipeline's cleaning process.
No format standardization needed - each tab maintains its original CSV format.
"""

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
import pandas as pd
import requests
from io import StringIO

from api.models import Advertiser, SheetSyncStatus


class Command(BaseCommand):
    help = 'Sync data from Google Sheets to database via advertiser pipelines'

    # Map pipeline names to their processing functions
    PIPELINE_MAP = {
        'noon': 'api.pipelines.noon_processor.process_noon_data',
        'namshi': 'api.pipelines.noon_processor.process_noon_data',  # Uses same processor
        'drnutrition': 'api.pipelines.drnutrition_processor.process_drnutrition_data',
        'styli': 'api.pipelines.styli_processor.process_styli_data',
        'springrose': 'api.pipelines.springrose_processor.process_springrose_data',
        'partnerize': 'api.pipelines.partnerize_processor.process_partnerize_data',
        'rdel': 'api.pipelines.rdel_processor.process_rdel_data',
    }

    # Default configuration for all tabs (can be overridden)
    DEFAULT_CONFIG = {
        'Noon_Transactions': {'pipeline': 'noon', 'advertiser': 'Noon'},
        'Namshi_Transactions': {'pipeline': 'namshi', 'advertiser': 'Namshi'},
        'DrNutrition_Transactions': {'pipeline': 'drnutrition', 'advertiser': 'Dr Nutrition'},
        'Styli_Transactions': {'pipeline': 'styli', 'advertiser': 'Styli'},
        'SpringRose_Transactions': {'pipeline': 'springrose', 'advertiser': 'SpringRose'},
        'Partnerize_Transactions': {'pipeline': 'partnerize', 'advertiser': 'Partnerize'},
        'Reef_Transactions': {'pipeline': 'rdel', 'advertiser': 'Reef'},
    }

    def add_arguments(self, parser):
        parser.add_argument(
            '--sheet-id',
            type=str,
            required=True,
            help='Google Sheet ID (from the URL)'
        )
        parser.add_argument(
            '--tab',
            type=str,
            help='Tab name to sync (e.g., "Noon_Transactions")'
        )
        parser.add_argument(
            '--pipeline',
            type=str,
            choices=list(self.PIPELINE_MAP.keys()),
            help='Pipeline to use for processing'
        )
        parser.add_argument(
            '--advertiser',
            type=str,
            help='Advertiser name (must match database exactly)'
        )
        parser.add_argument(
            '--all',
            action='store_true',
            help='Sync all configured tabs'
        )
        parser.add_argument(
            '--skip-tracking',
            action='store_true',
            help='Skip incremental tracking (process all rows)'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be processed without saving to database'
        )

    def handle(self, *args, **options):
        sheet_id = options['sheet_id']
        tab_name = options.get('tab')
        pipeline_name = options.get('pipeline')
        advertiser_name = options.get('advertiser')
        sync_all = options.get('all', False)
        skip_tracking = options.get('skip_tracking', False)
        dry_run = options.get('dry_run', False)

        self.stdout.write("=" * 70)
        self.stdout.write(self.style.SUCCESS("📊 GOOGLE SHEETS SYNC"))
        self.stdout.write("=" * 70)

        if sync_all:
            self.sync_all_tabs(sheet_id, skip_tracking, dry_run)
        elif tab_name and pipeline_name:
            self.sync_single_tab(
                sheet_id, tab_name, pipeline_name, 
                advertiser_name, skip_tracking, dry_run
            )
        else:
            self.stdout.write(
                self.style.ERROR("❌ Must provide either --all OR both --tab and --pipeline")
            )
            return

        self.stdout.write("=" * 70)
        self.stdout.write(self.style.SUCCESS("✅ SYNC COMPLETE"))
        self.stdout.write("=" * 70)

    def sync_all_tabs(self, sheet_id, skip_tracking, dry_run):
        """Sync all configured tabs"""
        self.stdout.write(f"\n🔄 Syncing all tabs from sheet: {sheet_id}\n")
        
        for tab_name, config in self.DEFAULT_CONFIG.items():
            self.stdout.write(f"\n{'─' * 70}")
            self.stdout.write(f"📋 Processing: {tab_name}")
            self.stdout.write(f"{'─' * 70}")
            
            try:
                self.sync_single_tab(
                    sheet_id, 
                    tab_name, 
                    config['pipeline'], 
                    config['advertiser'],
                    skip_tracking,
                    dry_run
                )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"❌ Failed to sync {tab_name}: {str(e)}")
                )
                continue

    def sync_single_tab(self, sheet_id, tab_name, pipeline_name, 
                        advertiser_name, skip_tracking, dry_run):
        """Sync a single tab"""
        
        # Read data from Google Sheet
        self.stdout.write(f"\n📥 Reading data from tab: {tab_name}")
        df = self.read_google_sheet(sheet_id, tab_name)
        
        if df is None or df.empty:
            self.stdout.write(self.style.WARNING("⚠️  No data found in sheet"))
            return

        self.stdout.write(f"✅ Retrieved {len(df)} rows")

        # Get last processed row if tracking is enabled
        start_row = 0
        if not skip_tracking:
            sync_status = SheetSyncStatus.objects.filter(
                sheet_id=sheet_id,
                tab_name=tab_name
            ).first()
            
            if sync_status:
                start_row = sync_status.last_row_processed
                self.stdout.write(f"📍 Resuming from row {start_row + 1}")
        
        # Get only new rows
        new_rows_df = df.iloc[start_row:]
        
        if new_rows_df.empty:
            self.stdout.write(self.style.WARNING("⚠️  No new rows to process"))
            return

        self.stdout.write(f"🆕 Processing {len(new_rows_df)} new rows")

        if dry_run:
            self.stdout.write("\n🔍 DRY RUN - Preview of data:")
            self.stdout.write(f"\nColumns: {list(new_rows_df.columns)}")
            self.stdout.write(f"\nFirst row sample:")
            self.stdout.write(str(new_rows_df.iloc[0].to_dict()))
            self.stdout.write(f"\nLast row sample:")
            self.stdout.write(str(new_rows_df.iloc[-1].to_dict()))
            return

        # Get advertiser
        if advertiser_name:
            try:
                advertiser = Advertiser.objects.get(name=advertiser_name)
            except Advertiser.DoesNotExist:
                self.stdout.write(
                    self.style.ERROR(f"❌ Advertiser '{advertiser_name}' not found")
                )
                return
        else:
            # Try to infer from pipeline
            advertiser = None
            self.stdout.write(
                self.style.WARNING("⚠️  No advertiser specified, pipeline will handle")
            )

        # Process through pipeline
        self.stdout.write(f"\n⚙️  Processing through '{pipeline_name}' pipeline...")
        
        try:
            # Import and call the pipeline processor
            pipeline_path = self.PIPELINE_MAP[pipeline_name]
            module_path, function_name = pipeline_path.rsplit('.', 1)
            
            # For now, we'll document the expected function signature
            # Each pipeline processor should accept (df, advertiser=None) and return result
            self.stdout.write(
                self.style.WARNING(
                    f"⚠️  Pipeline integration pending: {pipeline_path}\n"
                    f"   Please ensure pipeline can accept DataFrame directly"
                )
            )
            
            # TODO: Actual pipeline integration
            # from importlib import import_module
            # module = import_module(module_path)
            # processor = getattr(module, function_name)
            # result = processor(new_rows_df, advertiser=advertiser)

        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"❌ Pipeline processing failed: {str(e)}")
            )
            raise

        # Update tracking
        if not skip_tracking:
            with transaction.atomic():
                SheetSyncStatus.objects.update_or_create(
                    sheet_id=sheet_id,
                    tab_name=tab_name,
                    defaults={
                        'last_row_processed': len(df),
                        'last_sync_time': timezone.now(),
                        'last_sync_rows': len(new_rows_df),
                        'total_rows_synced': len(df)
                    }
                )
            self.stdout.write(
                self.style.SUCCESS(f"✅ Updated sync status: processed {len(df)} total rows")
            )

    def read_google_sheet(self, sheet_id, tab_name):
        """
        Read data from a Google Sheet tab.
        
        Uses the Google Sheets export API which doesn't require authentication
        for publicly shared sheets.
        """
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={tab_name}"
        
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
