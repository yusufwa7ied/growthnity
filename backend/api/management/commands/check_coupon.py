from django.core.management.base import BaseCommand
from api.models import Coupon, CampaignPerformance


class Command(BaseCommand):
    help = 'Check ALCOUPON data'

    def handle(self, *args, **options):
        # Check if coupon exists
        coupons = Coupon.objects.filter(code__icontains='ALCOUPON').select_related('advertiser', 'partner')
        
        self.stdout.write(f"\n=== COUPON TABLE ===")
        self.stdout.write(f"Found {coupons.count()} coupons matching 'ALCOUPON':\n")
        
        for c in coupons:
            self.stdout.write(f"Code: {c.code}")
            self.stdout.write(f"  Advertiser: {c.advertiser.name if c.advertiser else 'NULL'} (ID: {c.advertiser_id})")
            self.stdout.write(f"  Partner: {c.partner.name if c.partner else 'NULL'} (ID: {c.partner_id})")
            self.stdout.write(f"  Geo: {c.geo or 'NULL'}\n")
        
        # Check CampaignPerformance
        self.stdout.write(f"\n=== CAMPAIGN PERFORMANCE TABLE ===")
        perf = CampaignPerformance.objects.filter(coupon__code__icontains='ALCOUPON').select_related('advertiser', 'partner', 'coupon')
        
        self.stdout.write(f"Found {perf.count()} CampaignPerformance records:\n")
        
        for p in perf[:10]:  # Show first 10
            self.stdout.write(f"Date: {p.date}")
            self.stdout.write(f"  Advertiser: {p.advertiser.name if p.advertiser else 'NULL'} (ID: {p.advertiser_id})")
            self.stdout.write(f"  Partner: {p.partner.name if p.partner else 'NULL'} (ID: {p.partner_id})")
            self.stdout.write(f"  Coupon: {p.coupon.code if p.coupon else 'NULL'}")
            self.stdout.write(f"  Sales: {p.total_sales}, Revenue: {p.total_revenue}\n")
