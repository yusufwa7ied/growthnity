import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'backend.settings')
django.setup()

from api.models import NoonNamshiTransaction, CampaignPerformance
from datetime import date

today = date(2025, 11, 20)

print(f"🔍 Checking for Noon orders on {today}...")
print()

# Check NoonNamshiTransaction table
noon_transactions = NoonNamshiTransaction.objects.filter(
    order_date__date=today
)

print(f"📦 NoonNamshiTransaction table:")
print(f"   Found {noon_transactions.count()} transactions for today")

if noon_transactions.exists():
    print("\n   Sample transactions:")
    for tx in noon_transactions[:5]:
        print(f"   - Order: {tx.order_id} | Customer: {tx.customer_paid} AED | Coupon: {tx.coupon_code}")

# Check CampaignPerformance table
performance = CampaignPerformance.objects.filter(
    date=today,
    advertiser__name__in=['Noon', 'Namshi']
)

print(f"\n📊 CampaignPerformance table:")
print(f"   Found {performance.count()} performance rows for today")

if performance.exists():
    print("\n   Performance rows:")
    for cp in performance:
        print(f"   - Date: {cp.date} | Partner: {cp.partner.name if cp.partner else 'No Partner'}")
        print(f"     Orders: {cp.total_orders} | Sales: {cp.total_sales} AED")
        print(f"     Payout: {cp.total_payout} AED")
else:
    print("\n   ❌ No CampaignPerformance rows found for today")
    print("   This means the pipeline hasn't aggregated today's data yet")
    print("\n   💡 Run: python manage.py run_nn --start 2025-11-20 --end 2025-11-20")
