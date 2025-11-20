import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'backend.settings')
django.setup()

from api.models import NoonNamshiTransaction, Advertiser
from datetime import date

today = date(2025, 11, 20)

print("🔍 Checking Noon vs Namshi calculation methods...\n")

# Get today's transactions
noon_tx = NoonNamshiTransaction.objects.filter(
    created_date__date=today,
    advertiser_name="Noon"
)

namshi_tx = NoonNamshiTransaction.objects.filter(
    created_date__date=today,
    advertiser_name="Namshi"
)

print(f"📦 NOON Transactions (should use BRACKETS):")
print(f"   Count: {noon_tx.count()}")
if noon_tx.exists():
    for tx in noon_tx:
        print(f"\n   Order Value: {tx.sales} AED")
        print(f"   Revenue (our_rev): {tx.our_rev} AED")
        print(f"   Payout: {tx.payout} AED")
        print(f"   Partner: {tx.partner_name or '(No Partner)'}")
        print(f"   Coupon: {tx.coupon}")
        
        # Check if this looks like bracket calculation
        if tx.sales > 0:
            revenue_percent = (float(tx.our_rev) / float(tx.sales)) * 100
            print(f"   📊 Revenue as %: {revenue_percent:.2f}% (If < 5%, it's brackets ✅)")
        else:
            print("   ⚠️ Sales is 0")

print(f"\n📦 NAMSHI Transactions (should use PERCENTAGES):")
print(f"   Count: {namshi_tx.count()}")
if namshi_tx.exists():
    for tx in namshi_tx:
        print(f"\n   Order Value: {tx.sales} AED")
        print(f"   Revenue (our_rev): {tx.our_rev} AED")
        print(f"   Payout: {tx.payout} AED")
        print(f"   Partner: {tx.partner_name or '(No Partner)'}")
        
        if tx.sales > 0:
            revenue_percent = (float(tx.our_rev) / float(tx.sales)) * 100
            print(f"   📊 Revenue as %: {revenue_percent:.2f}% (Should be ~2-5% for percentages)")

print("\n" + "="*60)
print("✅ VERIFICATION:")
print("Noon should show LOW percentage (< 1%) = Using brackets")
print("Namshi should show NORMAL percentage (2-5%) = Using percentages")
print("="*60)
