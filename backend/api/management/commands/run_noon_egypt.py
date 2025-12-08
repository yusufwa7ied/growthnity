from django.core.management.base import BaseCommand
from datetime import datetime

from api.pipelines.noon_egypt import run


class Command(BaseCommand):
    help = "Run Noon Egypt pipeline (EGY) with bracket logic"

    def add_arguments(self, parser):
        parser.add_argument("--start", type=str, required=True, help="Start date YYYY-MM-DD")
        parser.add_argument("--end", type=str, required=True, help="End date YYYY-MM-DD")

    def handle(self, *args, **options):
        start_str = options["start"]
        end_str = options["end"]

        try:
            date_from = datetime.strptime(start_str, "%Y-%m-%d").date()
            date_to = datetime.strptime(end_str, "%Y-%m-%d").date()
        except ValueError:
            self.stderr.write("❌ Invalid date format. Use YYYY-MM-DD")
            return

        self.stdout.write(f"🚀 Running Noon Egypt pipeline {date_from} → {date_to}")

        count = run(date_from, date_to)

        self.stdout.write(f"✅ Done. Inserted {count} Noon Egypt rows.")
