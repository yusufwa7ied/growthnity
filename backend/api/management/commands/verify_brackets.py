"""
Management command to verify that Noon uses bracket-based calculations
while Namshi uses percentage-based calculations.
"""
from django.core.management.base import BaseCommand
from api.models import NoonNamshiTransaction
from datetime import date


class Command(BaseCommand):
    help = 'Verify Noon uses brackets and Namshi uses percentages'

    def handle(self, *args, **options):
        today = date(2025, 11, 20)
        
        self.stdout.write("🔍 Checking Noon vs Namshi calculation methods...\n")
        
        # Get today's transactions
        noon_tx = NoonNamshiTransaction.objects.filter(
            created_date__date=today,
            advertiser_name="Noon"
        )
        
        namshi_tx = NoonNamshiTransaction.objects.filter(
            created_date__date=today,
            advertiser_name="Namshi"
        )
        
        self.stdout.write(f"📊 Found {noon_tx.count()} Noon transactions and {namshi_tx.count()} Namshi transactions for {today}\n")
        
        # Analyze Noon transactions
        if noon_tx.count() > 0:
            self.stdout.write("\n" + "="*60)
            self.stdout.write("🌙 NOON TRANSACTIONS (Should use BRACKETS)")
            self.stdout.write("="*60 + "\n")
            
            for tx in noon_tx[:10]:  # Show first 10
                if tx.sales and float(tx.sales) > 0:
                    revenue_percent = (float(tx.our_rev) / float(tx.sales)) * 100
                    date_str = tx.created_date.date() if tx.created_date else 'N/A'
                    
                    self.stdout.write(f"\nOrder: {tx.order_id} ({date_str})")
                    self.stdout.write(f"  Coupon: {tx.coupon}")
                    self.stdout.write(f"  Sales: {tx.sales} {tx.currency}")
                    self.stdout.write(f"  Revenue: {tx.our_rev} {tx.currency}")
                    self.stdout.write(f"  Payout: {tx.payout} {tx.currency}")
                    self.stdout.write(f"  Revenue as %: {revenue_percent:.2f}% ", ending="")
                    
                    if revenue_percent < 1.0:
                        self.stdout.write(self.style.SUCCESS("✅ (Brackets confirmed - fixed amounts)"))
                    else:
                        self.stdout.write(self.style.WARNING("⚠️ (Looks like percentage-based?)"))
        else:
            self.stdout.write(self.style.WARNING("⚠️ No Noon transactions found"))
        
        # Analyze Namshi transactions
        if namshi_tx.count() > 0:
            self.stdout.write("\n" + "="*60)
            self.stdout.write("👔 NAMSHI TRANSACTIONS (Should use PERCENTAGES)")
            self.stdout.write("="*60 + "\n")
            
            for tx in namshi_tx[:10]:  # Show first 10
                if tx.sales and float(tx.sales) > 0:
                    revenue_percent = (float(tx.our_rev) / float(tx.sales)) * 100
                    date_str = tx.created_date.date() if tx.created_date else 'N/A'
                    
                    self.stdout.write(f"\nOrder: {tx.order_id} ({date_str})")
                    self.stdout.write(f"  Coupon: {tx.coupon}")
                    self.stdout.write(f"  Sales: {tx.sales} {tx.currency}")
                    self.stdout.write(f"  Revenue: {tx.our_rev} {tx.currency}")
                    self.stdout.write(f"  Payout: {tx.payout} {tx.currency}")
                    self.stdout.write(f"  Revenue as %: {revenue_percent:.2f}% ", ending="")
                    
                    if 2.0 <= revenue_percent <= 10.0:
                        self.stdout.write(self.style.SUCCESS("✅ (Percentage-based confirmed)"))
                    else:
                        self.stdout.write(self.style.WARNING("⚠️ (Unusual percentage)"))
        else:
            self.stdout.write(self.style.WARNING("⚠️ No Namshi transactions found"))
        
        self.stdout.write("\n" + "="*60)
        self.stdout.write("📝 SUMMARY")
        self.stdout.write("="*60)
        self.stdout.write(f"✅ If Noon shows < 1% revenue ratio → Brackets working")
        self.stdout.write(f"✅ If Namshi shows 2-10% revenue ratio → Percentages working")
        self.stdout.write("="*60 + "\n")
