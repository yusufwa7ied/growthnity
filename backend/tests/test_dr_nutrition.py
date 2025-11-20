import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'backend.settings')
django.setup()

import pandas as pd
from django.utils import timezone
from datetime import datetime
from api.models import Advertiser, Partner
from api.pipelines.helpers import resolve_payouts_with_history

print('🎬 DR. NUTRITION REAL EXAMPLE')
print('=' * 80)
print()

dr_nutrition = Advertiser.objects.get(name='Dr. Nutrition')
sherouk = Partner.objects.get(name='Sherouk Kadry')

print('📋 DR. NUTRITION RULES:')
print(f'   FTU (New Customer): {dr_nutrition.rev_ftu_rate}% commission')
print(f'   RTU (Returning): {dr_nutrition.rev_rtu_rate}% commission')
print()

test_data = [
    {'created_at': timezone.make_aware(datetime(2024, 11, 5, 10, 0)), 'sales': 500.0, 'orders': 1, 'user_type': 'FTU', 'coupon': 'SHEROUK10', 'country': 'AE'},
    {'created_at': timezone.make_aware(datetime(2024, 11, 6, 14, 30)), 'sales': 800.0, 'orders': 1, 'user_type': 'RTU', 'coupon': 'SHEROUK10', 'country': 'AE'},
]

df = pd.DataFrame(test_data)
df['partner_id'] = sherouk.id
df['partner_name'] = sherouk.name
df['advertiser_name'] = 'Dr. Nutrition'

result_df = resolve_payouts_with_history(dr_nutrition, df)

print('✅ RESULTS:')
print()
for idx, row in result_df.iterrows():
    print(f'Order {idx+1}: {row["user_type"]} - {row["sales"]} AED')
    print(f'  Revenue: {row["our_rev"]:.2f} AED')
    print(f'  Payout: {row["payout"]:.2f} AED')
    print(f'  Profit: {row["profit"]:.2f} AED')
    print()
