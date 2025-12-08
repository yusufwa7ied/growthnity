from django.core.management.base import BaseCommand
from api.models import CompanyUser, AccountAssignment, Partner, Advertiser


class Command(BaseCommand):
    help = 'Populate AccountAssignments for all TeamMembers with all partners and advertisers'

    def handle(self, *args, **options):
        # Get all users with TeamMember role
        team_members = CompanyUser.objects.filter(role__name="TeamMember")
        
        self.stdout.write(f"Found {team_members.count()} TeamMembers")
        
        # Get all partners and advertisers
        all_partners = Partner.objects.all()
        all_advertisers = Advertiser.objects.all()
        
        self.stdout.write(f"Found {all_partners.count()} partners")
        self.stdout.write(f"Found {all_advertisers.count()} advertisers")
        
        created_count = 0
        updated_count = 0
        
        for team_member in team_members:
            self.stdout.write(f"\nProcessing: {team_member.user.username}")
            
            # Get or create AccountAssignment for this team member
            assignment, created = AccountAssignment.objects.get_or_create(
                company_user=team_member
            )
            
            if created:
                self.stdout.write(f"  ✓ Created new AccountAssignment")
                created_count += 1
            else:
                self.stdout.write(f"  → AccountAssignment already exists")
                updated_count += 1
            
            # Find partner with matching name (case-insensitive)
            username = team_member.user.username.lower().replace('.', ' ').replace('_', ' ')
            matching_partners = Partner.objects.filter(name__icontains=username)
            
            if not matching_partners.exists():
                # Try with just first name or last name
                name_parts = username.split()
                if name_parts:
                    matching_partners = Partner.objects.filter(name__icontains=name_parts[0])
            
            if matching_partners.exists():
                assignment.partners.set(matching_partners)
                self.stdout.write(f"  → Partners: Assigned to {matching_partners.count()} matching partners: {[p.name for p in matching_partners]}")
            else:
                self.stdout.write(f"  → Partners: No matching partner found for '{team_member.user.username}'")
                assignment.partners.clear()
            
            # Add all advertisers
            advertisers_before = assignment.advertisers.count()
            assignment.advertisers.set(all_advertisers)
            advertisers_after = assignment.advertisers.count()
            self.stdout.write(f"  → Advertisers: {advertisers_before} → {advertisers_after}")
        
        self.stdout.write(self.style.SUCCESS(f"\n✅ Complete!"))
        self.stdout.write(self.style.SUCCESS(f"Created: {created_count} new assignments"))
        self.stdout.write(self.style.SUCCESS(f"Updated: {updated_count} existing assignments"))
