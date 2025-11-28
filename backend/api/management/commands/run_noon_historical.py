# backend/api/management/commands/run_noon_historical.py

from django.core.management.base import BaseCommand
from datetime import date
from api.pipelines.noon_historical import run


class Command(BaseCommand):
    help = "Run Noon historical data pipeline (pre-Nov 18, percentage-based)"

    def add_arguments(self, parser):
        parser.add_argument("--start", type=str, required=True, help="Start date (YYYY-MM-DD)")
        parser.add_argument("--end", type=str, required=True, help="End date (YYYY-MM-DD)")

    def handle(self, *args, **options):
        date_from = date.fromisoformat(options["start"])
        date_to = date.fromisoformat(options["end"])
        
        run(date_from, date_to)
