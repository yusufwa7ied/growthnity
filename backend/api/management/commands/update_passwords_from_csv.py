from django.core.management.base import BaseCommand
from django.contrib.auth.models import User
import csv


class Command(BaseCommand):
    help = 'Update partner passwords from CSV file'

    def add_arguments(self, parser):
        parser.add_argument('csv_file', type=str, help='Path to CSV file with email,password')

    def handle(self, *args, **options):
        csv_file = options['csv_file']
        
        updated = 0
        not_found = 0
        
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    user = User.objects.get(email=row['email'])
                    user.set_password(row['password'])
                    user.save()
                    self.stdout.write(f"✅ Updated: {row['email']}")
                    updated += 1
                except User.DoesNotExist:
                    self.stdout.write(self.style.WARNING(f"⚠️  Not found: {row['email']}"))
                    not_found += 1
        
        self.stdout.write(self.style.SUCCESS(f"\n✅ Updated {updated} passwords"))
        if not_found > 0:
            self.stdout.write(self.style.WARNING(f"⚠️  {not_found} users not found"))
