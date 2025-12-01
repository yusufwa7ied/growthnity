#!/usr/bin/env python
"""
Verification Test: Compare CampaignPerformance DB data with Google Sheets source
Tests last 7 days of data for revenue and payout accuracy
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
import gspread
from google.oauth2.service_account import Credentials


def get_google_sheets_data(advertiser_name, days=7):
    """Fetch raw data from Google Sheets for comparison"""
    
    # Mapping of advertiser names to sheet names
    sheet_mapping = {
        'Noon': 'Noon GCC',
        'Noon Egypt': 'Noon Egypt',
        'Styli': 'Styli',
        'Namshi': 'Namshi',
        'RDEL': 'RDEL'
    }
    
    if advertiser_name not in sheet_mapping:
        print(f"❌ No sheet mapping for advertiser: {advertiser_name}")
        return []
    
    sheet_name = sheet_mapping[advertiser_name]
    
    try:
        # Setup Google Sheets connection
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        SERVICE_ACCOUNT_FILE = '/app/google-service-account.json'
        
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        client = gspread.authorize(creds)
        
        # Open the spreadsheet
        SPREADSHEET_ID = '1cMm_9uSJIGJrXcdE1rCdSuQsn6tBPQJirnXQC8GfJSY'
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(sheet_name)
        
        # Get all records
        all_records = worksheet.get_all_records()
        
        # Filter for last N days
        cutoff_date = datetime.now().date() - timedelta(days=days)
        recent_records = []
        
        for record in all_records:
            date_str = record.get('Date', '')
            if not date_str:
                continue
            
            try:
                # Parse date (format: DD/MM/YYYY)
                record_date = datetime.strptime(date_str, '%d/%m/%Y').date()
                if record_date >= cutoff_date:
                    recent_records.append(record)
            except:
                continue
        
        return recent_records
        
    except Exception as e:
        print(f"❌ Error fetching Google Sheets data for {advertiser_name}: {e}")
        return []


def clean_decimal(value):
    """Clean and convert value to Decimal"""
    if not value:
        return Decimal('0')
    
    # Remove any currency symbols, commas, and whitespace
    if isinstance(value, str):
        value = value.replace('$', '').replace(',', '').replace(' ', '').strip()
    
    try:
        return Decimal(str(value))
    except:
        return Decimal('0')


def compare_advertiser_data(advertiser_name, days=7):
    """Compare DB data with Google Sheets data for one advertiser"""
    
    print(f"\n{'='*80}")
    print(f"🔍 Testing: {advertiser_name}")
    print(f"{'='*80}")
    
    # Get DB data
    cutoff_date = datetime.now().date() - timedelta(days=days)
    
    try:
        advertiser = Advertiser.objects.get(name=advertiser_name)
    except Advertiser.DoesNotExist:
        print(f"❌ Advertiser '{advertiser_name}' not found in database")
        return None
    
    db_records = CampaignPerformance.objects.filter(
        advertiser=advertiser,
        date__gte=cutoff_date
    ).order_by('date')
    
    print(f"📊 Database: Found {db_records.count()} records from {cutoff_date}")
    
    # Get Google Sheets data
    gs_records = get_google_sheets_data(advertiser_name, days=days)
    print(f"📄 Google Sheets: Found {len(gs_records)} records from {cutoff_date}")
    
    if not gs_records:
        print(f"⚠️  No Google Sheets data to compare")
        return None
    
    # Aggregate DB data by date
    db_by_date = {}
    for record in db_records:
        date_key = record.date.strftime('%Y-%m-%d')
        if date_key not in db_by_date:
            db_by_date[date_key] = {
                'revenue': Decimal('0'),
                'total_payout': Decimal('0'),
                'record_count': 0
            }
        
        db_by_date[date_key]['revenue'] += record.total_revenue or Decimal('0')
        db_by_date[date_key]['total_payout'] += record.total_payout or Decimal('0')
        db_by_date[date_key]['record_count'] += 1
    
    # Aggregate Google Sheets data by date
    gs_by_date = {}
    for record in gs_records:
        try:
            date_obj = datetime.strptime(record['Date'], '%d/%m/%Y').date()
            date_key = date_obj.strftime('%Y-%m-%d')
            
            if date_key not in gs_by_date:
                gs_by_date[date_key] = {
                    'revenue': Decimal('0'),
                    'total_payout': Decimal('0'),
                    'record_count': 0
                }
            
            # Sum up revenue and payouts (including any special payouts in the total)
            gs_by_date[date_key]['revenue'] += clean_decimal(record.get('Total Rev', 0))
            
            # Total Payout should include special payout if present
            total_payout = clean_decimal(record.get('Total Payout', 0))
            special_payout = clean_decimal(record.get('Special Payout', 0))
            gs_by_date[date_key]['total_payout'] += (total_payout + special_payout)
            gs_by_date[date_key]['record_count'] += 1
            
        except Exception as e:
            print(f"⚠️  Error processing record: {e}")
            continue
    
    # Compare date by date
    all_dates = sorted(set(list(db_by_date.keys()) + list(gs_by_date.keys())))
    
    print(f"\n{'Date':<12} {'Source':<8} {'Records':<10} {'Revenue':<20} {'Payout (incl. special)':<25}")
    print("-" * 80)
    
    total_db_revenue = Decimal('0')
    total_gs_revenue = Decimal('0')
    total_db_payout = Decimal('0')
    total_gs_payout = Decimal('0')
    
    mismatches = []
    
    for date_key in all_dates:
        db_data = db_by_date.get(date_key, {'revenue': Decimal('0'), 'total_payout': Decimal('0'), 'record_count': 0})
        gs_data = gs_by_date.get(date_key, {'revenue': Decimal('0'), 'total_payout': Decimal('0'), 'record_count': 0})
        
        print(f"{date_key:<12} {'DB':<8} {db_data['record_count']:<10} ${db_data['revenue']:>18.2f} ${db_data['total_payout']:>23.2f}")
        print(f"{'':<12} {'Sheets':<8} {gs_data['record_count']:<10} ${gs_data['revenue']:>18.2f} ${gs_data['total_payout']:>23.2f}")
        
        # Check for differences (allow 0.01 tolerance for rounding)
        revenue_diff = abs(db_data['revenue'] - gs_data['revenue'])
        payout_diff = abs(db_data['total_payout'] - gs_data['total_payout'])
        
        if revenue_diff > Decimal('0.01') or payout_diff > Decimal('0.01'):
            status = "❌ MISMATCH"
            mismatches.append({
                'date': date_key,
                'revenue_diff': revenue_diff,
                'payout_diff': payout_diff
            })
        else:
            status = "✅ Match"
        
        print(f"{'':<12} {status}")
        print()
        
        total_db_revenue += db_data['revenue']
        total_gs_revenue += gs_data['revenue']
        total_db_payout += db_data['total_payout']
        total_gs_payout += gs_data['total_payout']
    
    # Print totals
    print("=" * 80)
    print(f"{'TOTALS':<12} {'DB':<8} {'':<10} ${total_db_revenue:>18.2f} ${total_db_payout:>23.2f}")
    print(f"{'':<12} {'Sheets':<8} {'':<10} ${total_gs_revenue:>18.2f} ${total_gs_payout:>23.2f}")
    print(f"{'':<12} {'Diff':<8} {'':<10} ${abs(total_db_revenue - total_gs_revenue):>18.2f} ${abs(total_db_payout - total_gs_payout):>23.2f}")
    
    # Summary
    print("\n" + "=" * 80)
    if mismatches:
        print(f"❌ FAILED: {len(mismatches)} date(s) have mismatches")
        for mismatch in mismatches:
            print(f"   {mismatch['date']}: Rev diff=${mismatch['revenue_diff']:.2f}, Payout diff=${mismatch['payout_diff']:.2f}")
    else:
        print("✅ PASSED: All data matches between DB and Google Sheets!")
    
    return {
        'advertiser': advertiser_name,
        'passed': len(mismatches) == 0,
        'mismatches': len(mismatches),
        'total_dates': len(all_dates),
        'db_revenue': total_db_revenue,
        'gs_revenue': total_gs_revenue,
        'revenue_diff': abs(total_db_revenue - total_gs_revenue),
        'db_payout': total_db_payout,
        'gs_payout': total_gs_payout,
        'payout_diff': abs(total_db_payout - total_gs_payout)
    }


def main():
    print("\n" + "="*80)
    print("🧪 GOOGLE SHEETS DATA ACCURACY VERIFICATION TEST")
    print("="*80)
    print(f"Testing last 7 days of data")
    print(f"Comparing: Revenue, Total Payout, Special Payout")
    print("="*80)
    
    # Test all Google Sheets advertisers that exist in DB
    advertisers_to_test = ['Noon', 'Styli', 'Namshi']
    
    results = []
    for advertiser in advertisers_to_test:
        result = compare_advertiser_data(advertiser, days=7)
        if result:
            results.append(result)
    
    # Final Summary
    print("\n\n" + "="*80)
    print("📊 FINAL SUMMARY")
    print("="*80)
    
    total_passed = sum(1 for r in results if r['passed'])
    total_failed = len(results) - total_passed
    
    print(f"\nAdvertisers Tested: {len(results)}")
    print(f"✅ Passed: {total_passed}")
    print(f"❌ Failed: {total_failed}")
    
    if results:
        total_revenue_diff = sum(r['revenue_diff'] for r in results)
        total_payout_diff = sum(r['payout_diff'] for r in results)
        
        print(f"\nTotal Differences Across All Advertisers:")
        print(f"  Revenue:        ${total_revenue_diff:.2f}")
        print(f"  Total Payout:   ${total_payout_diff:.2f}")
        
        print("\nPer Advertiser Results:")
        print(f"{'Advertiser':<15} {'Status':<10} {'Rev Diff':<20} {'Payout Diff':<20}")
        print("-" * 70)
        for r in results:
            status = "✅ PASS" if r['passed'] else f"❌ FAIL ({r['mismatches']})"
            print(f"{r['advertiser']:<15} {status:<10} ${r['revenue_diff']:>18.2f} ${r['payout_diff']:>18.2f}")
    
    print("\n" + "="*80)
    if total_failed == 0:
        print("🎉 ALL TESTS PASSED! Data integrity verified.")
    else:
        print(f"⚠️  {total_failed} advertiser(s) have data mismatches. Review details above.")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
