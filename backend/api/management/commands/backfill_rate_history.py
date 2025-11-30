"""
Management command to backfill RevenueRuleHistory and PayoutRuleHistory
with current advertiser rates, using earliest transaction date as effective_date.
"""
from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import datetime
from api.models import (
    Advertiser, 
    RevenueRuleHistory, 
    PayoutRuleHistory,
    DrNutritionTransaction,
    StyliTransaction,
    SpringRoseTransaction,
    RDELTransaction,
    PartnerizeConversion,
    NoonNamshiTransaction,
    NoonTransaction
)


class Command(BaseCommand):
    help = 'Backfill RevenueRuleHistory and PayoutRuleHistory with current advertiser rates'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be created without actually creating records',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        
        if dry_run:
            self.stdout.write(self.style.WARNING('🔍 DRY RUN MODE - No records will be created\n'))
        
        advertisers = Advertiser.objects.all()
        
        for advertiser in advertisers:
            self.stdout.write(f'\n📊 Processing: {advertiser.name}')
            
            # Find earliest transaction date for this advertiser
            earliest_date = self.get_earliest_transaction_date(advertiser)
            
            if not earliest_date:
                self.stdout.write(self.style.WARNING(f'  ⚠️  No transactions found, using current date'))
                earliest_date = timezone.now()
            else:
                self.stdout.write(f'  📅 Earliest transaction: {earliest_date}')
            
            # Check if RevenueRuleHistory already exists
            revenue_exists = RevenueRuleHistory.objects.filter(advertiser=advertiser).exists()
            if revenue_exists:
                self.stdout.write(self.style.WARNING(f'  ⏭️  RevenueRuleHistory already exists, skipping'))
            else:
                if not dry_run:
                    RevenueRuleHistory.objects.create(
                        advertiser=advertiser,
                        effective_date=earliest_date,
                        rev_rate_type=advertiser.rev_rate_type or 'percent',
                        rev_ftu_rate=advertiser.rev_ftu_rate,
                        rev_rtu_rate=advertiser.rev_rtu_rate,
                        rev_ftu_fixed_bonus=advertiser.rev_ftu_fixed_bonus,
                        rev_rtu_fixed_bonus=advertiser.rev_rtu_fixed_bonus,
                        currency=advertiser.currency or 'AED',
                        exchange_rate=advertiser.exchange_rate,
                        assigned_by=None,
                        notes='Backfilled from current advertiser rates'
                    )
                    self.stdout.write(self.style.SUCCESS(f'  ✅ Created RevenueRuleHistory'))
                else:
                    self.stdout.write(f'  [DRY RUN] Would create RevenueRuleHistory:')
                    self.stdout.write(f'    - FTU Rate: {advertiser.rev_ftu_rate}%')
                    self.stdout.write(f'    - RTU Rate: {advertiser.rev_rtu_rate}%')
                    self.stdout.write(f'    - Effective: {earliest_date}')
            
            # Check if default PayoutRuleHistory already exists
            payout_exists = PayoutRuleHistory.objects.filter(
                advertiser=advertiser, 
                partner__isnull=True
            ).exists()
            
            if payout_exists:
                self.stdout.write(self.style.WARNING(f'  ⏭️  Default PayoutRuleHistory already exists, skipping'))
            else:
                # Only create if advertiser has default payout rates set
                if advertiser.default_ftu_payout or advertiser.default_rtu_payout:
                    if not dry_run:
                        PayoutRuleHistory.objects.create(
                            advertiser=advertiser,
                            partner=None,  # NULL = default for all partners
                            effective_date=earliest_date,
                            ftu_payout=advertiser.default_ftu_payout,
                            rtu_payout=advertiser.default_rtu_payout,
                            ftu_fixed_bonus=advertiser.default_ftu_fixed_bonus,
                            rtu_fixed_bonus=advertiser.default_rtu_fixed_bonus,
                            rate_type=advertiser.default_payout_rate_type or 'percent',
                            assigned_by=None,
                            notes='Backfilled from current advertiser default payouts'
                        )
                        self.stdout.write(self.style.SUCCESS(f'  ✅ Created default PayoutRuleHistory'))
                    else:
                        self.stdout.write(f'  [DRY RUN] Would create default PayoutRuleHistory:')
                        self.stdout.write(f'    - FTU Payout: {advertiser.default_ftu_payout}%')
                        self.stdout.write(f'    - RTU Payout: {advertiser.default_rtu_payout}%')
                        self.stdout.write(f'    - Effective: {earliest_date}')
                else:
                    self.stdout.write(self.style.WARNING(f'  ⏭️  No default payout rates set, skipping PayoutRuleHistory'))
        
        if dry_run:
            self.stdout.write(self.style.WARNING('\n🔍 DRY RUN COMPLETE - No records were created'))
            self.stdout.write('Run without --dry-run to actually create the records')
        else:
            self.stdout.write(self.style.SUCCESS('\n✅ Backfill complete!'))
    
    def get_earliest_transaction_date(self, advertiser):
        """Find the earliest transaction date across all transaction models"""
        earliest = None
        
        # Check DrNutrition
        dr_nut = DrNutritionTransaction.objects.filter(
            advertiser_name=advertiser.name
        ).order_by('created_date').first()
        if dr_nut and dr_nut.created_date:
            earliest = dr_nut.created_date if not earliest else min(earliest, dr_nut.created_date)
        
        # Check Styli
        styli = StyliTransaction.objects.filter(
            advertiser_name=advertiser.name
        ).order_by('created_date').first()
        if styli and styli.created_date:
            earliest = styli.created_date if not earliest else min(earliest, styli.created_date)
        
        # Check SpringRose
        spring = SpringRoseTransaction.objects.filter(
            advertiser_name=advertiser.name
        ).order_by('created_date').first()
        if spring and spring.created_date:
            earliest = spring.created_date if not earliest else min(earliest, spring.created_date)
        
        # Check RDEL (Reef, Daham, El_Esaei_Kids)
        rdel = RDELTransaction.objects.filter(
            advertiser=advertiser
        ).order_by('created_date').first()
        if rdel and rdel.created_date:
            earliest = rdel.created_date if not earliest else min(earliest, rdel.created_date)
        
        # Check Partnerize
        part = PartnerizeConversion.objects.filter(
            advertiser_name=advertiser.name
        ).order_by('conversion_time').first()
        if part and part.conversion_time:
            earliest = part.conversion_time if not earliest else min(earliest, part.conversion_time)
        
        # Check NoonNamshi
        noon_namshi = NoonNamshiTransaction.objects.filter(
            advertiser_name=advertiser.name
        ).order_by('created_date').first()
        if noon_namshi and noon_namshi.created_date:
            earliest = noon_namshi.created_date if not earliest else min(earliest, noon_namshi.created_date)
        
        # Check Noon
        noon = NoonTransaction.objects.filter(
            advertiser_name=advertiser.name
        ).order_by('order_date').first()
        if noon and noon.order_date:
            # Convert date to datetime
            noon_dt = timezone.make_aware(datetime.combine(noon.order_date, datetime.min.time()))
            earliest = noon_dt if not earliest else min(earliest, noon_dt)
        
        return earliest
