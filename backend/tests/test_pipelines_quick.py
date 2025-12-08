#!/usr/bin/env python
"""
Quick test to verify Namshi, Noon GCC, and Noon Egypt pipelines
Check revenue/payout calculations before and after November 1, 2025
"""
from api.models import NoonNamshiTransaction, NoonTransaction, CampaignPerformance
from datetime import date
from django.db.models import Sum, Count

print('\n' + '='*80)
print('TESTING PIPELINE DATA - NAMSHI, NOON GCC, NOON EGYPT')
print('='*80)

# Test 1: Namshi Transactions
print('\n📊 NAMSHI TRANSACTIONS (NoonNamshiTransaction)')
print('-'*80)
namshi_total = NoonNamshiTransaction.objects.count()
namshi_ftu = NoonNamshiTransaction.objects.filter(user_type='FTU').aggregate(
    count=Count('uuid'),
    revenue=Sum('our_rev'),
    payout=Sum('payout')
)
namshi_rtu = NoonNamshiTransaction.objects.filter(user_type='RTU').aggregate(
    count=Count('uuid'),
    revenue=Sum('our_rev'),
    payout=Sum('payout')
)
print(f'Total records: {namshi_total}')
print(f'FTU: {namshi_ftu["count"]} records, Revenue: ${namshi_ftu["revenue"]:.2f}, Payout: ${namshi_ftu["payout"]:.2f}')
print(f'RTU: {namshi_rtu["count"]} records, Revenue: ${namshi_rtu["revenue"]:.2f}, Payout: ${namshi_rtu["payout"]:.2f}')

# Test 2: Noon GCC Transactions (Before and After Nov 1)
print('\n📊 NOON GCC TRANSACTIONS (NoonTransaction - is_gcc=True)')
print('-'*80)
nov_1 = date(2025, 11, 1)
gcc_before_nov1 = NoonTransaction.objects.filter(is_gcc=True, order_date__lt=nov_1).aggregate(
    count=Count('id'),
    ftu_orders=Sum('ftu_orders'),
    rtu_orders=Sum('rtu_orders'),
    revenue=Sum('revenue_usd'),
    payout=Sum('payout_usd'),
    profit=Sum('profit_usd')
)
gcc_after_nov1 = NoonTransaction.objects.filter(is_gcc=True, order_date__gte=nov_1).aggregate(
    count=Count('id'),
    ftu_orders=Sum('ftu_orders'),
    rtu_orders=Sum('rtu_orders'),
    revenue=Sum('revenue_usd'),
    payout=Sum('payout_usd'),
    profit=Sum('profit_usd')
)

print(f'\n🔵 BEFORE NOV 1, 2025 (Old Logic - Percentage):')
print(f'  Records: {gcc_before_nov1["count"]}')
print(f'  FTU Orders: {gcc_before_nov1["ftu_orders"] or 0}, RTU Orders: {gcc_before_nov1["rtu_orders"] or 0}')
print(f'  Revenue: ${gcc_before_nov1["revenue"] or 0:.2f}')
print(f'  Payout: ${gcc_before_nov1["payout"] or 0:.2f}')
print(f'  Profit: ${gcc_before_nov1["profit"] or 0:.2f}')

print(f'\n🔵 AFTER NOV 1, 2025 (New Logic - Brackets):')
print(f'  Records: {gcc_after_nov1["count"]}')
print(f'  FTU Orders: {gcc_after_nov1["ftu_orders"] or 0}, RTU Orders: {gcc_after_nov1["rtu_orders"] or 0}')
print(f'  Revenue: ${gcc_after_nov1["revenue"] or 0:.2f}')
print(f'  Payout: ${gcc_after_nov1["payout"] or 0:.2f}')
print(f'  Profit: ${gcc_after_nov1["profit"] or 0:.2f}')

# Show sample records before and after
sample_before = NoonTransaction.objects.filter(is_gcc=True, order_date__lt=nov_1).order_by('order_date').first()
sample_after = NoonTransaction.objects.filter(is_gcc=True, order_date__gte=nov_1).order_by('order_date').first()

if sample_before:
    print(f'\n  Sample BEFORE Nov 1: Order {sample_before.order_id} on {sample_before.order_date}')
    print(f'    Country: {sample_before.country}, Orders: {sample_before.total_orders}')
    print(f'    Revenue: ${sample_before.revenue_usd:.2f}, Payout: ${sample_before.payout_usd:.2f}')
    
if sample_after:
    print(f'\n  Sample AFTER Nov 1: Order {sample_after.order_id} on {sample_after.order_date}')
    print(f'    Country: {sample_after.country}, Bracket: {sample_after.tier_bracket}')
    print(f'    Orders: {sample_after.total_orders}, Value: {sample_after.total_value}')
    print(f'    Revenue: ${sample_after.revenue_usd:.2f}, Payout: ${sample_after.payout_usd:.2f}')

# Test 3: Noon Egypt Transactions
print('\n📊 NOON EGYPT TRANSACTIONS (NoonTransaction - is_gcc=False)')
print('-'*80)
egypt_stats = NoonTransaction.objects.filter(is_gcc=False).aggregate(
    count=Count('id'),
    ftu_orders=Sum('ftu_orders'),
    rtu_orders=Sum('rtu_orders'),
    revenue=Sum('revenue_usd'),
    payout=Sum('payout_usd'),
    profit=Sum('profit_usd')
)
print(f'Total records: {egypt_stats["count"]}')
print(f'FTU Orders: {egypt_stats["ftu_orders"] or 0}, RTU Orders: {egypt_stats["rtu_orders"] or 0}')
print(f'Revenue: ${egypt_stats["revenue"] or 0:.2f}')
print(f'Payout: ${egypt_stats["payout"] or 0:.2f}')
print(f'Profit: ${egypt_stats["profit"] or 0:.2f}')

sample_egypt = NoonTransaction.objects.filter(is_gcc=False).order_by('order_date').first()
if sample_egypt:
    print(f'\n  Sample: Order {sample_egypt.order_id} on {sample_egypt.order_date}')
    print(f'    Bracket: {sample_egypt.tier_bracket}, Orders: {sample_egypt.total_orders}')
    print(f'    Revenue: ${sample_egypt.revenue_usd:.2f}, Payout: ${sample_egypt.payout_usd:.2f}')

# Test 4: CampaignPerformance Summary
print('\n📊 CAMPAIGN PERFORMANCE AGGREGATION')
print('-'*80)
namshi_perf = CampaignPerformance.objects.filter(advertiser__name='Namshi').aggregate(
    count=Count('id'),
    revenue=Sum('total_revenue'),
    payout=Sum('total_payout'),
    profit=Sum('total_our_rev')
)
noon_perf = CampaignPerformance.objects.filter(advertiser__name='Noon').aggregate(
    count=Count('id'),
    revenue=Sum('total_revenue'),
    payout=Sum('total_payout'),
    profit=Sum('total_our_rev')
)

print(f'Namshi: {namshi_perf["count"]} records, Revenue: ${namshi_perf["revenue"] or 0:.2f}, Profit: ${namshi_perf["profit"] or 0:.2f}')
print(f'Noon: {noon_perf["count"]} records, Revenue: ${noon_perf["revenue"] or 0:.2f}, Profit: ${noon_perf["profit"] or 0:.2f}')

print('\n' + '='*80)
print('✅ TEST COMPLETE')
print('='*80 + '\n')
