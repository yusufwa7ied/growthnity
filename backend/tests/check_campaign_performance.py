import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'backend.settings')
django.setup()

from api.models import CampaignPerformance, DrNutritionTransaction

# Get the transaction
tx = DrNutritionTransaction.objects.filter(order_id=2025199).first()

if tx:
    print('🔍 Looking for Order #2025199 in CampaignPerformance...')
    print(f'Transaction Date: {tx.created_date}')
    print(f'Partner: {tx.partner_name}')
    print(f'Advertiser: Dr. Nutrition')
    print()
    
    # Search in CampaignPerformance
    # Need to find partner by name since DrNutritionTransaction doesn't have partner_id FK
    from api.models import Partner
    partner = Partner.objects.filter(name=tx.partner_name).first()
    
    matches = CampaignPerformance.objects.filter(
        advertiser__name='Dr. Nutrition',
        partner=partner,
        date=tx.created_date.date()
    ) if partner else CampaignPerformance.objects.none()
    
    if matches.exists():
        print(f'✅ FOUND {matches.count()} matching row(s) in CampaignPerformance!')
        print()
        for cp in matches:
            print(f'📊 CampaignPerformance Record:')
            print(f'Date: {cp.date}')
            print(f'Partner: {cp.partner.name if cp.partner else "Unknown"}')
            print(f'Total Sales: {cp.total_sales} AED')
            print(f'Total Orders: {cp.total_orders}')
            print(f'FTU Orders: {cp.ftu_orders} | RTU Orders: {cp.rtu_orders}')
            print(f'Revenue (total_our_rev): {cp.total_our_rev} AED')
            print(f'Payout (total_payout): {cp.total_payout} AED')
            profit = cp.total_our_rev - cp.total_payout
            print(f'Profit: {profit} AED')
            print()
            print('💡 This is the AGGREGATED row that includes Order #2025199')
            print('   (may include other orders from same partner on same day)')
            print()
            print('🎯 BREAKDOWN:')
            print(f'   FTU Revenue: {cp.ftu_our_rev} AED | FTU Payout: {cp.ftu_payout} AED')
            print(f'   RTU Revenue: {cp.rtu_our_rev} AED | RTU Payout: {cp.rtu_payout} AED')
    else:
        print('❌ NOT FOUND in CampaignPerformance')
        print('   This means the pipeline has not been run yet for this date,')
        print('   or the transaction has not been aggregated into CampaignPerformance')
else:
    print('Transaction not found')
