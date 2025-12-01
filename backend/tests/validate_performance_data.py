#!/usr/bin/env python
"""
Simplified Data Integrity Test for Campaign Performance
Validates last 7 days of revenue and payout data in the database
"""

import os
import sys
import django
from datetime import datetime, timedelta
from decimal import Decimal

# Setup Django
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'backend.settings')
django.setup()

from api.models import CampaignPerformance, Advertiser


def validate_advertiser_data(advertiser_name, days=7):
    """Validate data completeness and consistency for one advertiser"""
    
    print(f"\n{'='*80}")
    print(f"🔍 Validating: {advertiser_name}")
    print(f"{'='*80}")
    
    cutoff_date = datetime.now().date() - timedelta(days=days)
    
    try:
        advertiser = Advertiser.objects.get(name=advertiser_name)
    except Advertiser.DoesNotExist:
        print(f"❌ Advertiser '{advertiser_name}' not found in database")
        return None
    
    records = CampaignPerformance.objects.filter(
        advertiser=advertiser,
        date__gte=cutoff_date
    ).order_by('date')
    
    total_count = records.count()
    print(f"📊 Found {total_count} performance records from {cutoff_date}")
    
    if total_count == 0:
        print(f"⚠️  No data found for the last {days} days")
        return None
    
    # Aggregate by date
    by_date = {}
    for record in records:
        date_key = record.date.strftime('%Y-%m-%d')
        if date_key not in by_date:
            by_date[date_key] = {
                'count': 0,
                'revenue': Decimal('0'),
                'payout': Decimal('0'),
                'orders': 0,
                'null_partner_count': 0,
                'null_coupon_count': 0
            }
        
        by_date[date_key]['count'] += 1
        by_date[date_key]['revenue'] += record.total_revenue or Decimal('0')
        by_date[date_key]['payout'] += record.total_payout or Decimal('0')
        by_date[date_key]['orders'] += record.total_orders or 0
        
        if not record.partner:
            by_date[date_key]['null_partner_count'] += 1
        if not record.coupon:
            by_date[date_key]['null_coupon_count'] += 1
    
    # Print daily breakdown
    print(f"\n{'Date':<12} {'Records':<10} {'Orders':<10} {'Revenue':<20} {'Payout':<20} {'Issues'}")
    print("-" * 90)
    
    total_revenue = Decimal('0')
    total_payout = Decimal('0')
    total_orders = 0
    issues_found = []
    
    for date_key in sorted(by_date.keys()):
        data = by_date[date_key]
        
        total_revenue += data['revenue']
        total_payout += data['payout']
        total_orders += data['orders']
        
        # Check for data quality issues
        issues = []
        if data['revenue'] == 0:
            issues.append("Zero revenue")
        if data['payout'] == 0:
            issues.append("Zero payout")
        if data['null_partner_count'] > 0:
            issues.append(f"{data['null_partner_count']} NULL partners")
        if data['null_coupon_count'] > 0:
            issues.append(f"{data['null_coupon_count']} NULL coupons")
        
        issues_str = ", ".join(issues) if issues else "✅"
        if issues:
            issues_found.append({'date': date_key, 'issues': issues})
        
        print(f"{date_key:<12} {data['count']:<10} {data['orders']:<10} ${data['revenue']:>18.2f} ${data['payout']:>18.2f} {issues_str}")
    
    # Summary
    print("=" * 90)
    print(f"{'TOTALS':<12} {total_count:<10} {total_orders:<10} ${total_revenue:>18.2f} ${total_payout:>18.2f}")
    print("=" * 90)
    
    # Sanity checks
    print("\n📋 Data Quality Checks:")
    checks_passed = 0
    checks_total = 0
    
    # Check 1: Revenue > 0
    checks_total += 1
    if total_revenue > 0:
        print(f"✅ Total revenue is positive: ${total_revenue:.2f}")
        checks_passed += 1
    else:
        print(f"❌ Total revenue is zero or negative!")
    
    # Check 2: Payout > 0
    checks_total += 1
    if total_payout > 0:
        print(f"✅ Total payout is positive: ${total_payout:.2f}")
        checks_passed += 1
    else:
        print(f"❌ Total payout is zero or negative!")
    
    # Check 3: Payout < Revenue (should always be true)
    checks_total += 1
    if total_payout < total_revenue:
        margin = total_revenue - total_payout
        margin_pct = (margin / total_revenue * 100) if total_revenue > 0 else 0
        print(f"✅ Payout less than revenue (margin: ${margin:.2f}, {margin_pct:.1f}%)")
        checks_passed += 1
    else:
        print(f"❌ Payout >= Revenue (this is wrong!)")
    
    # Check 4: Orders > 0
    checks_total += 1
    if total_orders > 0:
        print(f"✅ Total orders: {total_orders}")
        checks_passed += 1
    else:
        print(f"❌ Total orders is zero!")
    
    # Check 5: No dates with zero revenue
    checks_total += 1
    zero_revenue_dates = [date for date, data in by_date.items() if data['revenue'] == 0]
    if not zero_revenue_dates:
        print(f"✅ All dates have revenue")
        checks_passed += 1
    else:
        print(f"❌ {len(zero_revenue_dates)} date(s) with zero revenue: {', '.join(zero_revenue_dates)}")
    
    print(f"\n📊 Quality Score: {checks_passed}/{checks_total} checks passed ({checks_passed/checks_total*100:.0f}%)")
    
    if issues_found:
        print(f"\n⚠️  Data Quality Issues Found:")
        for issue in issues_found:
            print(f"   {issue['date']}: {', '.join(issue['issues'])}")
    
    return {
        'advertiser': advertiser_name,
        'total_records': total_count,
        'total_revenue': total_revenue,
        'total_payout': total_payout,
        'total_orders': total_orders,
        'date_count': len(by_date),
        'checks_passed': checks_passed,
        'checks_total': checks_total,
        'passed': checks_passed == checks_total
    }


def main():
    print("\n" + "="*80)
    print("🧪 CAMPAIGN PERFORMANCE DATA INTEGRITY TEST")
    print("="*80)
    print(f"Testing last 7 days of data")
    print(f"Validating: Revenue, Payout, Orders, Data Quality")
    print("="*80)
    
    # Test all active advertisers
    advertisers_to_test = ['Noon', 'Styli', 'Namshi', 'Dr. Nutrition', 'Spring Rose']
    
    results = []
    for advertiser in advertisers_to_test:
        result = validate_advertiser_data(advertiser, days=7)
        if result:
            results.append(result)
    
    # Final Summary
    print("\n\n" + "="*80)
    print("📊 FINAL SUMMARY")
    print("="*80)
    
    if not results:
        print("❌ No data found to validate")
        return
    
    total_passed = sum(1 for r in results if r['passed'])
    total_failed = len(results) - total_passed
    
    print(f"\nAdvertisers Tested: {len(results)}")
    print(f"✅ Passed All Checks: {total_passed}")
    print(f"❌ Failed Some Checks: {total_failed}")
    
    # Aggregate stats
    total_revenue = sum(r['total_revenue'] for r in results)
    total_payout = sum(r['total_payout'] for r in results)
    total_orders = sum(r['total_orders'] for r in results)
    total_records = sum(r['total_records'] for r in results)
    
    print(f"\nAggregate Stats (Last 7 Days):")
    print(f"  Total Records:  {total_records:,}")
    print(f"  Total Orders:   {total_orders:,}")
    print(f"  Total Revenue:  ${total_revenue:,.2f}")
    print(f"  Total Payout:   ${total_payout:,.2f}")
    margin = total_revenue - total_payout
    margin_pct = (margin / total_revenue * 100) if total_revenue > 0 else 0
    print(f"  Margin:         ${margin:,.2f} ({margin_pct:.1f}%)")
    
    print("\nPer Advertiser Results:")
    print(f"{'Advertiser':<20} {'Records':<10} {'Revenue':<20} {'Payout':<20} {'Quality'}")
    print("-" * 90)
    for r in results:
        quality = f"{r['checks_passed']}/{r['checks_total']}"
        status = "✅" if r['passed'] else "⚠️ "
        print(f"{r['advertiser']:<20} {r['total_records']:<10} ${r['total_revenue']:>18,.2f} ${r['total_payout']:>18,.2f} {status} {quality}")
    
    print("\n" + "="*80)
    if total_failed == 0:
        print("🎉 ALL TESTS PASSED! Data integrity verified.")
    else:
        print(f"⚠️  {total_failed} advertiser(s) have quality issues. Review details above.")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
