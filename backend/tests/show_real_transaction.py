import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'backend.settings')
django.setup()

from api.models import DrNutritionTransaction, Advertiser, CampaignPerformance

print('🎬 REAL TRANSACTION: Dr. Nutrition Order #2025199')
print('=' * 80)
print()

tx = DrNutritionTransaction.objects.filter(order_id=2025199).first()
dr_nutrition = Advertiser.objects.get(name='Dr. Nutrition')

print('📦 STEP 1: CUSTOMER PURCHASE')
print(f'Date: {tx.created_date}')
print(f'Customer bought: {tx.sales:.2f} AED')
print(f'Coupon: {tx.coupon}')
print(f'Type: {tx.user_type} (Returning User)')
print('🏷️ STEP 2: PARTNER IDENTIFICATION')
print(f'Partner: {tx.partner_name}')
print(f'Type: {tx.partner_type}')
print()

print('💰 STEP 3: REVENUE CALCULATION')
print(f'RTU Rate: {dr_nutrition.rev_rtu_rate}%')
print(f'Calculation: {tx.sales:.2f} x {dr_nutrition.rev_rtu_rate}% = {tx.our_rev:.2f} AED')
print(f'Dr. Nutrition pays YOU: {tx.our_rev:.2f} AED')
print()

print('💸 STEP 4: PAYOUT CALCULATION')
print(f'Payout Rate: {tx.rtu_rate}%')
print(f'Calculation: {tx.our_rev:.2f} x {tx.rtu_rate}% = {tx.payout:.2f} AED')
print(f'YOU pay {tx.partner_name}: {tx.payout:.2f} AED')
print()

print('🏆 STEP 5: YOUR PROFIT')
print(f'Revenue: {tx.our_rev:.2f} AED')
print(f'Payout: {tx.payout:.2f} AED')
print(f'Profit: {tx.profit:.2f} AED')
print()

print('💱 STEP 6: USD CONVERSION')
print(f'Exchange: 0.27 USD per AED')
print(f'Payout USD: {tx.payout_usd:.2f} USD')
print(f'Profit USD: {tx.profit_usd:.2f} USD')
print()

print('🌐 STEP 7: DASHBOARD VIEW')
print(f'{tx.partner_name} logs in and sees:')
print(f'  Date: Nov 18')
print(f'  Earned: ${tx.payout_usd:.2f} USD')
print()
print('=' * 80)
