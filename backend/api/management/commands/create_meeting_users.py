from django.core.management.base import BaseCommand
from django.contrib.auth.models import User
from api.models import CompanyUser, CompanyRole
import secrets
import string


class Command(BaseCommand):
    help = 'Create users for the meeting with OpsManager access'

    def handle(self, *args, **options):
        users_to_create = [
            'taha@growthnity.com',
            'tarek@growthnity.com',
            'herman@growthnity.com',
            'tjamus83@gmail.com',
            'malcom@growthnity.com',
            'A.ellaban@growthnity.com',
            'Osama@growthnity.com',
        ]

        credentials = []

        # Get or create OpsManager role
        ops_manager_role, _ = CompanyRole.objects.get_or_create(name='OpsManager')

        for email in users_to_create:
            # Generate a secure random password
            password = ''.join(secrets.choice(string.ascii_letters + string.digits + string.punctuation) for _ in range(12))
            
            # Extract username from email
            username = email.split('@')[0]
            
            # Check if user already exists
            if User.objects.filter(username=username).exists():
                self.stdout.write(self.style.WARNING(f'User {username} already exists, skipping...'))
                continue
            
            # Create the Django user
            user = User.objects.create_user(
                username=username,
                email=email,
                password=password,
                first_name=username.title(),
                last_name='Guest'
            )
            
            # Create CompanyUser with OpsManager role
            company_user = CompanyUser.objects.create(
                user=user,
                role=ops_manager_role
            )
            
            credentials.append({
                'email': email,
                'username': username,
                'password': password
            })
            
            self.stdout.write(self.style.SUCCESS(f'Created user: {email} with OpsManager role'))
        
        # Print credentials
        self.stdout.write('\n' + '='*60)
        self.stdout.write(self.style.SUCCESS('USER CREDENTIALS FOR MEETING:'))
        self.stdout.write('='*60 + '\n')
        
        for cred in credentials:
            self.stdout.write(f"Email: {cred['email']}")
            self.stdout.write(f"Username: {cred['username']}")
            self.stdout.write(f"Password: {cred['password']}")
            self.stdout.write(f"Role: OpsManager")
            self.stdout.write('-'*60)
