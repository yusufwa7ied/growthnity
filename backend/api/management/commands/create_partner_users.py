from django.core.management.base import BaseCommand
from django.contrib.auth.models import User
from django.db import transaction
from api.models import Partner, CompanyUser, CompanyRole, AccountAssignment


class Command(BaseCommand):
    help = 'Create user accounts for partners (affiliates/influencers) with email addresses'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be created without actually creating users',
        )
        parser.add_argument(
            '--partner-type',
            type=str,
            help='Filter by partner type: AFF, INF, or MB',
        )
        parser.add_argument(
            '--reset-passwords',
            action='store_true',
            help='Reset passwords for existing partner users',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        partner_type_filter = options['partner_type']
        reset_passwords = options['reset_passwords']

        # Get TeamMember role (partners will be TeamMembers)
        try:
            team_member_role = CompanyRole.objects.get(name='TeamMember')
            self.stdout.write(self.style.SUCCESS(f'✅ Using TeamMember role'))
        except CompanyRole.DoesNotExist:
            self.stdout.write(self.style.ERROR(f'❌ TeamMember role not found! Please create it first.'))
            return

        # Get partners with emails
        partners = Partner.objects.filter(email__isnull=False).exclude(email='')
        
        if partner_type_filter:
            partners = partners.filter(partner_type=partner_type_filter)

        self.stdout.write(f"\n📊 Found {partners.count()} partners with email addresses\n")

        created_count = 0
        updated_count = 0
        skipped_count = 0

        for partner in partners:
            # Username is the full email address
            username = partner.email.lower()
            
            # Check if user already exists (by username OR by email)
            existing_user = User.objects.filter(username=username).first()
            if not existing_user:
                existing_user = User.objects.filter(email=partner.email).first()
            
            if existing_user:
                # User exists - check if linked to this partner
                try:
                    company_user = CompanyUser.objects.get(user=existing_user)
                    assignment = company_user.accountassignment_set.first()
                    
                    if assignment and assignment.partners.filter(id=partner.id).exists():
                        # Already linked to this partner
                        if reset_passwords:
                            # Reset password
                            temp_password = f"{partner.name.replace(' ', '')}@123"
                            if not dry_run:
                                existing_user.set_password(temp_password)
                                existing_user.save()
                                self.stdout.write(self.style.SUCCESS(
                                    f'🔑 Reset password for {partner.name} ({partner.email})'
                                ))
                                updated_count += 1
                            else:
                                self.stdout.write(self.style.WARNING(
                                    f'[DRY RUN] Would reset password for {partner.name} ({partner.email})'
                                ))
                        else:
                            self.stdout.write(self.style.WARNING(
                                f'⏭️  Skipped {partner.name} - user already exists and linked'
                            ))
                            skipped_count += 1
                    else:
                        # User exists but not linked to this partner - link them
                        if not dry_run:
                            if not assignment:
                                assignment = AccountAssignment.objects.create(company_user=company_user)
                            assignment.partners.add(partner)
                            
                            # Update department if needed
                            if partner.partner_type == 'AFF':
                                company_user.department = 'affiliate'
                            elif partner.partner_type == 'INF':
                                company_user.department = 'influencer'
                            elif partner.partner_type == 'MB':
                                company_user.department = 'media_buying'
                            company_user.save()
                            
                            self.stdout.write(self.style.SUCCESS(
                                f'🔗 Linked existing user to partner: {partner.name} ({partner.email})'
                            ))
                            updated_count += 1
                        else:
                            self.stdout.write(self.style.WARNING(
                                f'[DRY RUN] Would link existing user to partner: {partner.name}'
                            ))
                            
                except CompanyUser.DoesNotExist:
                    # User exists but no CompanyUser - create CompanyUser and link
                    if not dry_run:
                        with transaction.atomic():
                            company_user = CompanyUser.objects.create(
                                user=existing_user,
                                role=team_member_role,
                                department='affiliate' if partner.partner_type == 'AFF' else 'influencer' if partner.partner_type == 'INF' else 'media_buying',
                                phone=partner.phone
                            )
                            
                            assignment = AccountAssignment.objects.create(company_user=company_user)
                            assignment.partners.add(partner)
                            
                            self.stdout.write(self.style.SUCCESS(
                                f'🔗 Created CompanyUser for existing Django user: {partner.name} ({partner.email})'
                            ))
                            updated_count += 1
                    else:
                        self.stdout.write(self.style.WARNING(
                            f'[DRY RUN] Would create CompanyUser for existing user: {partner.name}'
                        ))
            else:
                # Create new user
                temp_password = f"{partner.name.replace(' ', '')}@123"
                
                if not dry_run:
                    with transaction.atomic():
                        # Create Django user with email as username
                        user = User.objects.create_user(
                            username=username,
                            email=partner.email,
                            password=temp_password,
                            first_name=partner.name.split()[0] if partner.name else '',
                            last_name=' '.join(partner.name.split()[1:]) if len(partner.name.split()) > 1 else ''
                        )
                        
                        # Create CompanyUser as TeamMember
                        company_user = CompanyUser.objects.create(
                            user=user,
                            role=team_member_role,
                            department='affiliate' if partner.partner_type == 'AFF' else 'influencer' if partner.partner_type == 'INF' else 'media_buying',
                            phone=partner.phone
                        )
                        
                        # Create AccountAssignment linking to partner
                        assignment = AccountAssignment.objects.create(company_user=company_user)
                        assignment.partners.add(partner)
                        
                        self.stdout.write(self.style.SUCCESS(
                            f'✅ Created user for {partner.name} ({partner.partner_type}) | Email: {partner.email} | Password: {temp_password}'
                        ))
                        created_count += 1
                else:
                    self.stdout.write(self.style.WARNING(
                        f'[DRY RUN] Would create user for {partner.name} ({partner.partner_type}) | Email: {partner.email}'
                    ))
                    created_count += 1

        self.stdout.write(f"\n{'='*80}")
        self.stdout.write(self.style.SUCCESS(f"\n📈 SUMMARY:"))
        self.stdout.write(f"  ✅ Created: {created_count}")
        self.stdout.write(f"  🔄 Updated/Linked: {updated_count}")
        self.stdout.write(f"  ⏭️  Skipped: {skipped_count}")
        
        if dry_run:
            self.stdout.write(self.style.WARNING(f"\n⚠️  This was a DRY RUN - no users were actually created."))
            self.stdout.write(self.style.WARNING(f"    Run without --dry-run to create users."))
        else:
            self.stdout.write(self.style.SUCCESS(f"\n✅ Users processed successfully!"))
            self.stdout.write(f"\n📧 Partner login details:")
            self.stdout.write(f"   - Website: https://growthnity-app.com")
            self.stdout.write(f"   - Username: their email address")
            self.stdout.write(f"   - Default password format: PartnerName@123")
            self.stdout.write(f"   - Role: TeamMember")
            self.stdout.write(f"   - Department: affiliate/influencer/media_buying")
            self.stdout.write(f"   - Multi-device login: ✅ Supported (Django handles this automatically)")
