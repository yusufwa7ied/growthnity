from django.core.management.base import BaseCommand
from api.pipelines.rdel import run_rdel_pipeline


class Command(BaseCommand):
    help = "Run RDEL (Reef, Daham, El_Esaei_Kids) pipeline for a date range"

    def add_arguments(self, parser):
        parser.add_argument("--start", type=str, required=True, help="Start date YYYY-MM-DD")
        parser.add_argument("--end", type=str, required=True, help="End date YYYY-MM-DD")

    def handle(self, *args, **options):
        start_str = options["start"]
        end_str = options["end"]

        self.stdout.write(f"🚀 Running RDEL pipeline {start_str} → {end_str}")

        run_rdel_pipeline(start_str, end_str)

        self.stdout.write("✅ RDEL pipeline completed.")
