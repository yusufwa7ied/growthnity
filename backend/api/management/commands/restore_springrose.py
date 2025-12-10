"""
Restore Spring Rose October-November data from snapshot 238
and merge with December data.
"""
from django.core.management.base import BaseCommand
from django.db import transaction
from api.models import SpringRoseTransaction, RawAdvertiserRecord, Advertiser, Partner, PartnerPayout
from datetime import datetime
from decimal import Decimal
import json


class Command(BaseCommand):
    help = "Restore Spring Rose Oct-Nov data from snapshot 238, merge with Dec data"

    def add_arguments(self, parser):
        parser.add_argument(
            '--snapshot-id',
            type=int,
            default=238,
            help='Snapshot ID to restore from (default: 238)'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Preview changes without applying them'
        )

    def handle(self, *args, **options):
        snapshot_id = options['snapshot_id']
        dry_run = options['dry_run']

        self.stdout.write(self.style.WARNING(f"\n{'DRY RUN - ' if dry_run else ''}Spring Rose Data Restore"))
        self.stdout.write(self.style.WARNING("=" * 70))

        # Get snapshot
        try:
            snapshot = RawAdvertiserRecord.objects.get(id=snapshot_id)
            self.stdout.write(f"✓ Found snapshot {snapshot_id}")
            self.stdout.write(f"  Fetched: {snapshot.date_fetched}")
            self.stdout.write(f"  Period: {snapshot.date_from} → {snapshot.date_to}")
        except RawAdvertiserRecord.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"✗ Snapshot {snapshot_id} not found"))
            return

        # Get Spring Rose advertiser
        try:
            advertiser = Advertiser.objects.get(name__iexact="Spring Rose")
        except Advertiser.DoesNotExist:
            self.stdout.write(self.style.ERROR("✗ Spring Rose advertiser not found"))
            return

        # Extract Oct-Nov data from snapshot
        rows = snapshot.data.get('rows', [])
        oct_nov_orders = [
            row for row in rows
            if 'Oct' in row.get('Created At', '') or 'Nov' in row.get('Created At', '')
        ]

        self.stdout.write(f"\n📊 Data Summary:")
        self.stdout.write(f"  Total rows in snapshot: {len(rows)}")
        self.stdout.write(f"  Oct-Nov orders: {len(oct_nov_orders)}")

        # Count current data
        current_total = SpringRoseTransaction.objects.count()
        current_dec = SpringRoseTransaction.objects.filter(
            created_date__month=12,
            created_date__year=2025
        ).count()
        current_nov = SpringRoseTransaction.objects.filter(
            created_date__month=11,
            created_date__year=2025
        ).count()
        current_oct = SpringRoseTransaction.objects.filter(
            created_date__month=10,
            created_date__year=2025
        ).count()

        self.stdout.write(f"\n📋 Current Database State:")
        self.stdout.write(f"  Total transactions: {current_total}")
        self.stdout.write(f"  October: {current_oct}")
        self.stdout.write(f"  November: {current_nov}")
        self.stdout.write(f"  December: {current_dec}")

        if dry_run:
            self.stdout.write(self.style.WARNING(f"\n🔍 DRY RUN - No changes will be made"))
            self.stdout.write(f"\nProposed actions:")
            self.stdout.write(f"  1. Delete all October transactions ({current_oct} records)")
            self.stdout.write(f"  2. Delete all November transactions ({current_nov} records)")
            self.stdout.write(f"  3. Insert {len(oct_nov_orders)} Oct-Nov orders from snapshot")
            self.stdout.write(f"  4. Keep December data ({current_dec} records)")
            self.stdout.write(f"\nFinal count would be: {len(oct_nov_orders)} + {current_dec} = {len(oct_nov_orders) + current_dec} transactions")
            return

        # Confirm action
        self.stdout.write(self.style.WARNING(f"\n⚠️  This will:"))
        self.stdout.write(f"  • DELETE {current_oct + current_nov} Oct-Nov transactions")
        self.stdout.write(f"  • INSERT {len(oct_nov_orders)} Oct-Nov orders from snapshot")
        self.stdout.write(f"  • KEEP {current_dec} December transactions")
        
        confirm = input("\nType 'yes' to proceed: ")
        if confirm.lower() != 'yes':
            self.stdout.write(self.style.ERROR("Aborted"))
            return

        # Execute restore
        with transaction.atomic():
            # Delete Oct-Nov data
            deleted_oct = SpringRoseTransaction.objects.filter(
                created_date__month=10,
                created_date__year=2025
            ).delete()[0]
            
            deleted_nov = SpringRoseTransaction.objects.filter(
                created_date__month=11,
                created_date__year=2025
            ).delete()[0]

            self.stdout.write(f"\n🗑️  Deleted:")
            self.stdout.write(f"  October: {deleted_oct} records")
            self.stdout.write(f"  November: {deleted_nov} records")

            # Process and insert Oct-Nov orders
            inserted = 0
            skipped = 0
            errors = 0

            for row in oct_nov_orders:
                try:
                    order_id = row.get('# Order ID', '').strip()
                    if not order_id:
                        skipped += 1
                        continue

                    # Parse date
                    created_at_str = row.get('Created At', '').strip()
                    created_date = self.parse_date(created_at_str)

                    # Extract coupon and find partner
                    coupon_code = row.get('Coupon Code', '').strip()
                    partner = None
                    if coupon_code:
                        try:
                            from api.models import Coupon
                            coupon_obj = Coupon.objects.filter(
                                code__iexact=coupon_code,
                                advertiser=advertiser
                            ).first()
                            if coupon_obj:
                                partner = coupon_obj.partner
                        except:
                            pass

                    # Parse sales amount
                    price_str = row.get('Total Price', '').replace('ر.س', '').strip()
                    try:
                        sales = Decimal(price_str)
                    except:
                        sales = Decimal('0')

                    # Determine user type (assume RTU for existing orders)
                    user_type = 'RTU'
                    ftu_orders = 0
                    rtu_orders = 1

                    # Calculate revenue and payout
                    rev_rate = advertiser.rev_rtu_rate if advertiser.rev_rate_type == 'percent' else Decimal('0')
                    our_rev = (sales * rev_rate / 100) if advertiser.rev_rate_type == 'percent' else Decimal('0')

                    # Get payout rate
                    payout_rate = Decimal('0')
                    if partner:
                        payout_rule = PartnerPayout.objects.filter(
                            partner=partner,
                            advertiser=advertiser
                        ).first()
                        if payout_rule:
                            payout_rate = payout_rule.rtu_payout

                    payout = (our_rev * payout_rate / 100) if payout_rate else Decimal('0')
                    profit = our_rev - payout

                    # Create transaction
                    SpringRoseTransaction.objects.create(
                        order_id=order_id,
                        created_date=created_date,
                        delivery_status=row.get('Status', 'Delivered'),
                        country='SA',  # Spring Rose is Saudi Arabia
                        coupon=coupon_code,
                        user_type=user_type,
                        partner_name=partner.name if partner else 'Unknown',
                        partner_type=partner.partner_type if partner else 'AFF',
                        advertiser_name='Spring Rose',
                        currency='SAR',
                        rate_type=advertiser.rev_rate_type,
                        sales=sales,
                        commission=Decimal('0'),
                        our_rev=our_rev,
                        ftu_orders=ftu_orders,
                        rtu_orders=rtu_orders,
                        orders=1,
                        ftu_rate=Decimal('0'),
                        rtu_rate=payout_rate,
                        payout=payout,
                        profit=profit,
                        payout_usd=payout / advertiser.exchange_rate,
                        profit_usd=profit / advertiser.exchange_rate
                    )
                    inserted += 1

                except Exception as e:
                    errors += 1
                    self.stdout.write(self.style.ERROR(f"  Error processing {order_id}: {str(e)}"))

            self.stdout.write(f"\n✅ Restore Complete:")
            self.stdout.write(f"  Inserted: {inserted} orders")
            if skipped:
                self.stdout.write(f"  Skipped: {skipped} (missing data)")
            if errors:
                self.stdout.write(self.style.ERROR(f"  Errors: {errors}"))

            # Final count
            final_total = SpringRoseTransaction.objects.count()
            final_oct = SpringRoseTransaction.objects.filter(
                created_date__month=10,
                created_date__year=2025
            ).count()
            final_nov = SpringRoseTransaction.objects.filter(
                created_date__month=11,
                created_date__year=2025
            ).count()
            final_dec = SpringRoseTransaction.objects.filter(
                created_date__month=12,
                created_date__year=2025
            ).count()

            self.stdout.write(f"\n📊 Final Database State:")
            self.stdout.write(f"  Total transactions: {final_total}")
            self.stdout.write(f"  October: {final_oct}")
            self.stdout.write(f"  November: {final_nov}")
            self.stdout.write(f"  December: {final_dec}")
            self.stdout.write(self.style.SUCCESS(f"\n✓ Data restore successful!"))

    def parse_date(self, date_str):
        """Parse Spring Rose date format like 'Oct 20, 2025\n11:25 AM'"""
        if not date_str:
            return None
        
        # Clean up the string
        date_str = date_str.replace('\n', ' ').strip()
        
        try:
            # Try parsing with time
            return datetime.strptime(date_str, '%b %d, %Y %I:%M %p')
        except:
            try:
                # Try without time
                parts = date_str.split()
                if len(parts) >= 3:
                    date_part = ' '.join(parts[:3])
                    return datetime.strptime(date_part, '%b %d, %Y')
            except:
                pass
        
        return None
