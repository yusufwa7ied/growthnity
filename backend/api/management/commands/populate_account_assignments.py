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
            
            # Add all partners
            partners_before = assignment.partners.count()
            assignment.partners.set(all_partners)
            partners_after = assignment.partners.count()
            self.stdout.write(f"  → Partners: {partners_before} → {partners_after}")
            
            # Add all advertisers
            advertisers_before = assignment.advertisers.count()
            assignment.advertisers.set(all_advertisers)
            advertisers_after = assignment.advertisers.count()
            self.stdout.write(f"  → Advertisers: {advertisers_before} → {advertisers_after}")
        
        self.stdout.write(self.style.SUCCESS(f"\n✅ Complete!"))
        self.stdout.write(self.style.SUCCESS(f"Created: {created_count} new assignments"))
        self.stdout.write(self.style.SUCCESS(f"Updated: {updated_count} existing assignments"))
