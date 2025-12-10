"""
Test Power BI connection and explore Reef dataset structure.

Usage:
    python manage.py test_powerbi
"""

from django.core.management.base import BaseCommand
from django.conf import settings
from api.services.powerbi_service import powerbi_service


class Command(BaseCommand):
    help = 'Test Power BI connection and explore Reef dataset'

    def handle(self, *args, **options):
        self.stdout.write("=" * 70)
        self.stdout.write(self.style.SUCCESS("🔌 TESTING POWER BI CONNECTION"))
        self.stdout.write("=" * 70)
        
        try:
            # Test 1: Get access token
            self.stdout.write("\n📝 Step 1: Getting access token...")
            token = powerbi_service._get_access_token()
            self.stdout.write(self.style.SUCCESS(f"✅ Token obtained: {token[:50]}..."))
            
            # Test 2: Get dataset ID from report
            self.stdout.write("\n📝 Step 2: Finding dataset ID...")
            report_id = settings.REEF_POWERBI_REPORT_ID
            group_id = settings.REEF_POWERBI_GROUP_ID
            
            dataset_id = powerbi_service.get_dataset_id_from_report(group_id, report_id)
            self.stdout.write(self.style.SUCCESS(f"✅ Dataset ID: {dataset_id}"))
            
            # Save dataset ID for later use
            self.stdout.write(f"\n💡 Add this to settings.py:")
            self.stdout.write(f"   REEF_POWERBI_DATASET_ID = '{dataset_id}'")
            
            # Test 3: List all datasets in workspace
            self.stdout.write("\n📝 Step 3: Listing all datasets in workspace...")
            datasets = powerbi_service.list_datasets(group_id)
            self.stdout.write(f"✅ Found {len(datasets)} dataset(s):")
            for ds in datasets:
                self.stdout.write(f"   - {ds['name']} (ID: {ds['id']})")
            
            # Test 4: Try to get table structure
            self.stdout.write("\n📝 Step 4: Exploring dataset structure...")
            try:
                tables_df = powerbi_service.get_tables_in_dataset(group_id, dataset_id)
                if not tables_df.empty:
                    self.stdout.write(self.style.SUCCESS(f"✅ Found {len(tables_df)} table(s):"))
                    self.stdout.write(str(tables_df))
                else:
                    self.stdout.write(self.style.WARNING("⚠️ Could not retrieve table structure via INFO.TABLES()"))
                    self.stdout.write("   This is normal - we'll discover columns by querying data directly.")
            except Exception as e:
                self.stdout.write(self.style.WARNING(f"⚠️ Could not get table structure: {e}"))
                self.stdout.write("   This is normal - we'll discover structure by querying data.")
            
            # Test 5: Try a simple DAX query to discover columns
            self.stdout.write("\n📝 Step 5: Testing sample DAX query...")
            
            # Try common table names for Reef data
            table_names = ['Reef', 'Data', 'Sales', 'Orders', 'Transactions', 'ReefData']
            
            for table_name in table_names:
                try:
                    self.stdout.write(f"\n   Trying table: '{table_name}'...")
                    dax_query = f"EVALUATE TOPN(5, '{table_name}')"
                    df = powerbi_service.execute_dax_query(group_id, dataset_id, dax_query)
                    
                    if not df.empty:
                        self.stdout.write(self.style.SUCCESS(f"✅ Found table '{table_name}'!"))
                        self.stdout.write(f"\n   Columns: {list(df.columns)}")
                        self.stdout.write(f"\n   Sample data (first 5 rows):")
                        self.stdout.write(str(df.head()))
                        
                        self.stdout.write(f"\n\n💡 Table '{table_name}' structure discovered!")
                        self.stdout.write(f"   Columns: {', '.join(df.columns)}")
                        break
                except Exception as e:
                    self.stdout.write(f"   ❌ Table '{table_name}' not found or error: {str(e)[:100]}")
            else:
                self.stdout.write(self.style.WARNING("\n⚠️ Could not auto-discover table name."))
                self.stdout.write("   Please check the Power BI report to see the table/column names.")
                self.stdout.write("   Then we can create a custom DAX query.")
            
            self.stdout.write("\n" + "=" * 70)
            self.stdout.write(self.style.SUCCESS("✅ POWER BI CONNECTION TEST COMPLETE!"))
            self.stdout.write("=" * 70)
            
            self.stdout.write("\n📋 Next Steps:")
            self.stdout.write("1. If table structure found above, we can create the pipeline!")
            self.stdout.write("2. If not found, check Power BI report for table/column names")
            self.stdout.write("3. Run: python manage.py run_reef_powerbi --start 2025-12-01 --end 2025-12-10")
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"\n❌ ERROR: {e}"))
            self.stdout.write(f"\n{type(e).__name__}: {str(e)}")
            
            import traceback
            self.stdout.write(self.style.ERROR("\nFull traceback:"))
            self.stdout.write(traceback.format_exc())
            
            self.stdout.write("\n💡 Troubleshooting:")
            self.stdout.write("1. Check if Azure AD app has correct permissions (Report.Read.All, Dataset.Read.All)")
            self.stdout.write("2. Ensure admin consent was granted in Azure portal")
            self.stdout.write("3. Verify credentials in settings.py are correct")
            self.stdout.write("4. Check if Power BI service principal is enabled in tenant settings")
