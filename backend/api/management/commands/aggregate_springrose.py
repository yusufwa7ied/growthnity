"""
Aggregate Spring Rose transaction data to CampaignPerformance table
"""
from django.core.management.base import BaseCommand
from api.pipelines.springrose import push_springrose_to_performance
from datetime import date


class Command(BaseCommand):
    help = "Aggregate Spring Rose transactions to CampaignPerformance"

    def add_arguments(self, parser):
        parser.add_argument("--start", type=str, default="2025-10-20", help="Start date YYYY-MM-DD")
        parser.add_argument("--end", type=str, default="2025-12-31", help="End date YYYY-MM-DD")

    def handle(self, *args, **options):
        from datetime import datetime
        
        start_str = options["start"]
        end_str = options["end"]

        try:
            date_from = datetime.strptime(start_str, "%Y-%m-%d").date()
            date_to = datetime.strptime(end_str, "%Y-%m-%d").date()
        except ValueError:
            self.stderr.write(self.style.ERROR("❌ Invalid date format. Use YYYY-MM-DD"))
            return

        self.stdout.write(self.style.SUCCESS(f"🔄 Aggregating Spring Rose data {date_from} → {date_to}"))
        
        result = push_springrose_to_performance(date_from, date_to)
        
        self.stdout.write(self.style.SUCCESS(f"✅ Created {result} CampaignPerformance records"))
        self.stdout.write(self.style.SUCCESS("Dashboard should now show the restored data!"))
