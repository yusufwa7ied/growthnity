from django.core.management.base import BaseCommand
from api.models import CompanyUser


class Command(BaseCommand):
    help = 'List all CompanyUsers with their roles and departments'

    def handle(self, *args, **options):
        users = CompanyUser.objects.select_related('role', 'user').all()
        
        self.stdout.write(f"\nTotal CompanyUsers: {users.count()}\n")
        self.stdout.write("=" * 80)
        
        for u in users:
            username = u.user.username if u.user else "NO USER"
            role = u.role.name if u.role else "NO ROLE"
            dept = u.department or "NO DEPT"
            
            self.stdout.write(f"{username:30} | Role: {role:15} | Dept: {dept}")
