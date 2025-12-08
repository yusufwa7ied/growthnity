# backend/api/pipelines/noon_egypt.py
"""
Pipeline for processing Noon EGYPT orders ONLY.
Uses bracket-based payouts (already working).
"""

import pandas as pd
from datetime import date, datetime
from decimal import Decimal
from django.db import transaction
from django.conf import settings

from api.models import (
    Advertiser,
    NoonTransaction,
    CampaignPerformance,
    Partner,
    Coupon,
)

from api.pipelines.helpers import (
    store_raw_snapshot,
    enrich_df,
    nf,
    nz,
)
from api.services.s3_service import s3_service

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------

ADVERTISER_NAME = "Noon"
S3_EGYPT_KEY = settings.S3_PIPELINE_FILES["noon_egypt"]

# Date cutoff for bracket-based logic
BRACKET_START_DATE = datetime(2025, 11, 18).date()

# Egypt Default Payouts (Bracket-based)
EGYPT_DEFAULT_PAYOUTS = {
    "Bracket 1": 0.20,   # $4.75 - $14.25
    "Bracket 2": 0.55,   # $14.26 - $23.85
    "Bracket 3": 1.00,   # $23.86 - $37.24
    "Bracket 4": 1.70,   # $37.25 - $59.40
    "Bracket 5": 2.50,   # $59.41 - $72.00
    "Bracket 6": 3.25,   # $72.01 - $110.00
    "Bracket 7": 5.50,   # $110.01 & above
}


# ---------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------

def extract_bracket_number(bracket_str):
    """
    Extract bracket number from Egypt bracket string.
    Examples: "Bracket 2_$0.68" → "Bracket 2", "Bracket 7_$5.50" → "Bracket 7"
    """
    if pd.isna(bracket_str) or not bracket_str:
        return None
    
    try:
        # Split by "_" and take first part
        return bracket_str.split("_")[0].strip()
    except:
        return None


def extract_bracket_revenue(bracket_str):
    """
    Extract revenue from Egypt bracket string.
    Examples: "Bracket 2_$0.68" → 0.68, "Bracket 7_$5.50" → 5.50
    """
    if pd.isna(bracket_str) or not bracket_str:
        return 0.0
    
    try:
        # Split by "_$" and take second part
        value = bracket_str.split("_$")[1].strip()
        return float(value)
    except:
        return 0.0


# ---------------------------------------------------
# CLEANING / NORMALIZATION
# ---------------------------------------------------

def clean_noon_egypt(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean Egypt CSV data.
    Expected columns: ID, Date, Tag, Coupon Code, #order, Bracket, order_value_gmv_usd
    """
    print("🧹 Cleaning Noon Egypt data...")
    
    # Rename standard columns
    df = df.rename(columns={
        "Date": "order_date",
        "Coupon Code": "coupon_code",
        "#order": "order_hash",  # This is order ID, not count
        "Bracket": "tier",
        "order_value_gmv_usd": "total_value",
        "Tag": "platform",
    })
    
    # Parse date (format: "Nov 24, 2025")
    df["order_date"] = pd.to_datetime(df["order_date"], format="%b %d, %Y", errors="coerce").dt.date
    
    # Egypt data has 1 row per order, so total_orders = 1 for each row
    df["total_orders"] = 1
    df["non_payable_orders"] = 0
    df["payable_orders"] = 1
    
    # Numeric conversions
    df["total_value"] = pd.to_numeric(df["total_value"], errors="coerce").fillna(0)
    
    # Egypt doesn't have FTU/RTU breakdown
    df["ftu_orders"] = 0
    df["ftu_value"] = 0
    df["rtu_orders"] = 0
    df["rtu_value"] = 0
    
    # String fields
    df["coupon_code"] = df["coupon_code"].astype(str).str.strip()
    
    if "platform" in df.columns:
        df["platform"] = df["platform"].astype(str).str.strip()
    else:
        df["platform"] = ""
        
    if "country" in df.columns:
        df["country"] = df["country"].astype(str).str.strip()
    else:
        df["country"] = "eg"
    
    # Add region flags
    df["is_gcc"] = False
    df["region"] = "egypt"
    
    # Add created_at for enrichment (use order_date as datetime)
    df["created_at"] = pd.to_datetime(df["order_date"])
    
    # Filter out zero orders
    df = df[df["payable_orders"] > 0].copy()
    
    print(f"✅ Cleaned {len(df)} Egypt rows")
    return df


# ---------------------------------------------------
# CALCULATE REVENUE & PAYOUT
# ---------------------------------------------------

def calculate_financials(df: pd.DataFrame, advertiser: Advertiser) -> pd.DataFrame:
    """
    Calculate revenue and payout for each row based on date and region.
    """
    print("💰 Calculating revenue and payout...")
    
    results = []
    
    for idx, row in df.iterrows():
        order_date = row["order_date"]
        is_gcc = row["is_gcc"]
        tier = row["tier"]
        partner_name = row.get("partner_name", "")
        payable_orders = int(row["payable_orders"])
        
        # Pre-Nov 18: Percentage-based (already calculated for total, not per order)
        if order_date < BRACKET_START_DATE:
            revenue_usd = calculate_pre_nov18_revenue(
                row["ftu_value"], 
                row["rtu_value"],
                currency="AED" if is_gcc else "USD"
            )
            payout_usd = calculate_pre_nov18_payout(
                row["ftu_value"],
                row["rtu_value"],
                currency="AED" if is_gcc else "USD"
            )
        else:
            # Post-Nov 18: Bracket-based (PER ORDER, need to multiply by payable_orders)
            if is_gcc:
                # Extract revenue per order from tier
                revenue_per_order = extract_tier_revenue(tier)
                
                # Check for special payout per order
                special_payout = get_special_payout(partner_name, tier, is_gcc, order_date)
                if special_payout:
                    payout_per_order = special_payout
                else:
                    payout_per_order = GCC_DEFAULT_PAYOUTS.get(tier, 0)
                
                # Multiply by number of payable orders
                revenue_usd = revenue_per_order * payable_orders
                payout_usd = payout_per_order * payable_orders
            else:
                # Egypt: Use tier/bracket to get payout per order
                tier_str = str(tier)
                if "_$" in tier_str:
                    revenue_per_order = extract_bracket_revenue(tier_str)
                elif "$" in tier_str:
                    revenue_per_order = float(tier_str.replace("$", "").strip())
                else:
                    revenue_per_order = 0
                
                # Egypt: payout = revenue, multiply by payable orders
                revenue_usd = revenue_per_order * payable_orders
                payout_usd = revenue_usd
        
        # Calculate our revenue (profit)
        our_rev_usd = round(revenue_usd - payout_usd, 2)
        
        results.append({
            **row.to_dict(),
            "revenue_usd": revenue_usd,
            "payout_usd": payout_usd,
            "our_rev_usd": our_rev_usd,
            "profit_usd": our_rev_usd,
        })
    
    result_df = pd.DataFrame(results)
    print(f"✅ Calculated financials for {len(result_df)} rows")
    return result_df


# ---------------------------------------------------
# SAVE TO DATABASE
# ---------------------------------------------------

def save_transactions(advertiser: Advertiser, df: pd.DataFrame, date_from: date, date_to: date, region: str) -> int:
    """
    Save processed transactions to NoonTransaction table for specific region.
    """
    print(f"💾 Saving Noon {region.upper()} transactions...")
    
    # Delete existing records in date range for this region only
    deleted_count, _ = NoonTransaction.objects.filter(
        order_date__gte=date_from,
        order_date__lte=date_to,
        region=region,
    ).delete()
    
    if deleted_count > 0:
        print(f"🗑️  Deleted {deleted_count} existing {region.upper()} records")
    
    # Prepare records for bulk insert
    records = []
    for idx, row in df.iterrows():
        # Get coupon object
        coupon = None
        coupon_id = row.get("coupon_id")
        if pd.notna(coupon_id) and coupon_id:
            try:
                coupon = Coupon.objects.get(id=int(coupon_id))
            except (Coupon.DoesNotExist, ValueError, TypeError):
                pass
        
        # Get partner object
        partner = None
        partner_id = row.get("partner_id")
        if pd.notna(partner_id) and partner_id:
            try:
                partner = Partner.objects.get(id=int(partner_id))
            except (Partner.DoesNotExist, ValueError, TypeError):
                pass
        
        # Determine user type
        ftu_orders = int(row.get("ftu_orders", 0))
        rtu_orders = int(row.get("rtu_orders", 0))
        if ftu_orders > 0 and rtu_orders > 0:
            user_type = "MIXED"
        elif ftu_orders > 0:
            user_type = "FTU"
        elif rtu_orders > 0:
            user_type = "RTU"
        else:
            user_type = ""
        
        record = NoonTransaction(
            order_id=f"noon_{row['region']}_{row['order_date']}_{row['coupon_code']}_{idx}",
            order_date=row["order_date"],
            advertiser_name="Noon",
            is_gcc=row["is_gcc"],
            region=row["region"],
            platform=row.get("platform", ""),
            country=row.get("country", ""),
            coupon=coupon,
            coupon_code=row["coupon_code"],
            tier_bracket=row.get("tier", ""),
            total_orders=int(row.get("total_orders", 0)),
            non_payable_orders=int(row.get("non_payable_orders", 0)),
            payable_orders=int(row.get("payable_orders", 0)),
            total_value=Decimal(str(row.get("total_value", 0))),
            ftu_orders=ftu_orders,
            ftu_value=Decimal(str(row.get("ftu_value", 0))),
            rtu_orders=rtu_orders,
            rtu_value=Decimal(str(row.get("rtu_value", 0))),
            partner=partner,
            partner_name=row.get("partner_name", ""),
            revenue_usd=Decimal(str(row.get("revenue_usd", 0))),
            payout_usd=Decimal(str(row.get("payout_usd", 0))),
            our_rev_usd=Decimal(str(row.get("our_rev_usd", 0))),
            profit_usd=Decimal(str(row.get("profit_usd", 0))),
            user_type=user_type,
        )
        records.append(record)
    
    # Bulk insert
    NoonTransaction.objects.bulk_create(records, batch_size=500)
    print(f"✅ Saved {len(records)} Noon transactions")
    
    return len(records)


# ---------------------------------------------------
# MAIN RUN FUNCTION
# ---------------------------------------------------

def run(date_from: date, date_to: date):
    """
    Main pipeline execution function.
    Processes ONLY Egypt data.
    """
    advertiser = Advertiser.objects.get(name=ADVERTISER_NAME)
    
    print(f"🚀 Running Noon EGYPT pipeline {date_from} → {date_to}")
    print(f"📅 Bracket logic applies from: {BRACKET_START_DATE}")
    
    total_count = 0
    
    # ---------------------------------------------------
    # PROCESS EGYPT DATA
    # ---------------------------------------------------
    try:
        print("\n" + "="*60)
        print("📍 PROCESSING NOON EGYPT DATA")
        print("="*60)
        
        # Load from S3
        print(f"📄 Loading Egypt CSV from S3: {S3_EGYPT_KEY}")
        egypt_df = s3_service.read_csv_to_df(S3_EGYPT_KEY)
        print(f"✅ Loaded {len(egypt_df)} Egypt rows")
        
        # Clean
        egypt_clean = clean_noon_egypt(egypt_df)
        
        # Filter date range
        egypt_clean = egypt_clean[
            (egypt_clean["order_date"] >= date_from) & 
            (egypt_clean["order_date"] <= date_to)
        ].copy()
        print(f"📊 Filtered to {len(egypt_clean)} Egypt rows in date range")
        
        if len(egypt_clean) > 0:
            # Add coupon column for enrichment (enrich_df expects "coupon")
            egypt_clean["coupon"] = egypt_clean["coupon_code"]
            
            # Enrich with partners
            egypt_enriched = enrich_df(egypt_clean, advertiser=advertiser)
            
            # Calculate financials
            egypt_final = calculate_financials(egypt_enriched, advertiser)
            
            # Save
            egypt_count = save_transactions(advertiser, egypt_final, date_from, date_to, "egypt")
            total_count += egypt_count
            print(f"✅ Egypt: {egypt_count} transactions saved")
        else:
            print("⚠️  No Egypt data in date range")
    
    except Exception as e:
        print(f"⚠️  Egypt processing failed: {e}")
        import traceback
        traceback.print_exc()
    
    # ---------------------------------------------------
    # PUSH TO PERFORMANCE
    # ---------------------------------------------------
    push_noon_to_performance(date_from, date_to)
    
    # ---------------------------------------------------
    # SUMMARY
    # ---------------------------------------------------
    print("\n" + "="*60)
    print(f"✅ Noon EGYPT pipeline completed: {total_count} total transactions")
    print("="*60)
    
    return total_count


# ---------------------------------------------------
# PUSH TO CAMPAIGN PERFORMANCE
# ---------------------------------------------------

def push_noon_to_performance(date_from: date, date_to: date):
    """
    Aggregate NoonTransaction data and push to CampaignPerformance table.
    """
    print("\n📊 Aggregating Noon data to CampaignPerformance...")
    
    advertiser = Advertiser.objects.filter(name=ADVERTISER_NAME).first()
    if not advertiser:
        print("⚠️  Noon advertiser not found")
        return 0
    
    qs = NoonTransaction.objects.filter(
        order_date__gte=date_from,
        order_date__lte=date_to
    )
    
    if not qs.exists():
        print("⚠️ No NoonTransaction rows found")
        return 0
    
    # Group by date, partner, coupon, region
    groups = {}
    
    for r in qs:
        # Get coupon object
        coupon_obj = r.coupon
        
        key = (
            r.order_date,
            r.partner_name,
            coupon_obj.code if coupon_obj else r.coupon_code,
            r.region,
        )
        
        if key not in groups:
            groups[key] = {
                "date": r.order_date,
                "partner": r.partner,
                "partner_name": r.partner_name,
                "coupon": coupon_obj,
                "coupon_code": r.coupon_code,
                "geo": r.region,
                "ftu_orders": 0,
                "rtu_orders": 0,
                "ftu_sales": 0,
                "rtu_sales": 0,
                "ftu_revenue": 0,
                "rtu_revenue": 0,
                "ftu_payout": 0,
                "rtu_payout": 0,
            }
        
        g = groups[key]
        
        # Calculate sales in USD (total_value already in USD or converted)
        sales_usd = float(r.total_value) if r.is_gcc else float(r.total_value)
        if r.is_gcc:
            # Convert AED to USD
            sales_usd = sales_usd * AED_TO_USD
        
        # Aggregate by user type
        if r.ftu_orders > 0:
            g["ftu_orders"] += r.ftu_orders
            g["ftu_sales"] += sales_usd
            g["ftu_revenue"] += float(r.revenue_usd)
            g["ftu_payout"] += float(r.payout_usd)
        
        if r.rtu_orders > 0:
            g["rtu_orders"] += r.rtu_orders
            g["rtu_sales"] += sales_usd
            g["rtu_revenue"] += float(r.revenue_usd)
            g["rtu_payout"] += float(r.payout_usd)
        
        # If no FTU/RTU breakdown, count as total
        if r.ftu_orders == 0 and r.rtu_orders == 0:
            # Egypt data has no FTU/RTU breakdown
            g["rtu_orders"] += r.payable_orders
            g["rtu_sales"] += sales_usd
            g["rtu_revenue"] += float(r.revenue_usd)
            g["rtu_payout"] += float(r.payout_usd)
    
    # Delete existing performance records
    deleted = CampaignPerformance.objects.filter(
        advertiser=advertiser,
        date__gte=date_from,
        date__lte=date_to
    ).delete()
    print(f"🗑️  Deleted {deleted[0]} existing CampaignPerformance rows for Noon")
    
    # Create new performance records
    records = []
    for key, g in groups.items():
        total_orders = g["ftu_orders"] + g["rtu_orders"]
        total_sales = g["ftu_sales"] + g["rtu_sales"]
        total_revenue = g["ftu_revenue"] + g["rtu_revenue"]
        total_payout = g["ftu_payout"] + g["rtu_payout"]
        
        record = CampaignPerformance(
            advertiser=advertiser,
            partner=g["partner"],
            coupon=g["coupon"],
            date=g["date"],
            geo=g["geo"],
            total_orders=total_orders,
            ftu_orders=g["ftu_orders"],
            rtu_orders=g["rtu_orders"],
            ftu_sales=round(g["ftu_sales"], 2),
            rtu_sales=round(g["rtu_sales"], 2),
            total_sales=round(total_sales, 2),
            ftu_revenue=round(g["ftu_revenue"], 2),
            rtu_revenue=round(g["rtu_revenue"], 2),
            total_revenue=round(total_revenue, 2),
            ftu_payout=round(g["ftu_payout"], 2),
            rtu_payout=round(g["rtu_payout"], 2),
            total_payout=round(total_payout, 2),
            ftu_our_rev=round(g["ftu_revenue"] - g["ftu_payout"], 2),
            rtu_our_rev=round(g["rtu_revenue"] - g["rtu_payout"], 2),
            total_our_rev=round(total_revenue - total_payout, 2),
        )
        records.append(record)
    
    CampaignPerformance.objects.bulk_create(records, batch_size=500)
    print(f"✅ Aggregated {len(records)} performance rows")
    
    return len(records)
