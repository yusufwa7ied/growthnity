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
    NoonEgyptTransaction,
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

ADVERTISER_NAME = "Noon_Egypt"
S3_EGYPT_KEY = settings.S3_PIPELINE_FILES["noon_egypt"]

# Date cutoff for bracket-based logic
BRACKET_START_DATE = datetime(2025, 11, 18).date()

# Exchange rate for AED to USD
AED_TO_USD = 0.27

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
    
    # Rename columns to match our model
    df = df.rename(columns={
        "ID": "record_id",
        "Date": "order_date",
        "Tag": "tag",
        "Coupon Code": "coupon_code",
        "#order": "order_hash",
        "Bracket": "bracket",
        "order_value_gmv_usd": "order_value_usd",
    })
    
    # Parse date
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    
    # Clean data types
    df["record_id"] = df["record_id"].astype(str)
    df["order_hash"] = df["order_hash"].astype(str)
    df["coupon_code"] = df["coupon_code"].astype(str).str.strip().str.upper()
    df["tag"] = df["tag"].astype(str).str.lower().str.strip()
    df["bracket"] = df["bracket"].astype(str).str.strip()
    df["order_value_usd"] = pd.to_numeric(df["order_value_usd"], errors="coerce").fillna(0.0)
    
    # Filter out rows with invalid dates
    df = df[df["order_date"].notna()].copy()
    
    # Calculate revenue (we get 15% of GMV)
    df["revenue_usd"] = df["order_value_usd"] * 0.15
    
    # Extract payout from bracket string (e.g., "Bracket 1_$0.27" → 0.27)
    df["payout_usd"] = df["bracket"].apply(extract_bracket_revenue)
    
    # Calculate profit
    df["profit_usd"] = df["revenue_usd"] - df["payout_usd"]
    
    # Add created_at for enrichment (use order_date as datetime)
    df["created_at"] = df["order_date"]
    
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
    Save processed transactions to NoonEgyptTransaction table.
    """
    print(f"💾 Saving Noon Egypt transactions...")
    
    # Delete existing records in date range
    deleted_count, _ = NoonEgyptTransaction.objects.filter(
        order_date__gte=date_from,
        order_date__lte=date_to,
    ).delete()
    
    if deleted_count > 0:
        print(f"🗑️  Deleted {deleted_count} existing Egypt records")
    
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
        
        # Determine user type from the Tag field
        user_type = str(row.get("tag", "")).lower().strip()
        if user_type not in ["ftu", "rtu"]:
            user_type = "rtu"  # Default to rtu
        
        # Extract bracket payout
        bracket_str = str(row.get("bracket", ""))
        bracket_payout = extract_bracket_revenue(bracket_str)
        
        record = NoonEgyptTransaction(
            record_id=str(row.get("record_id", "")),
            order_hash=str(row.get("order_hash", "")),
            order_date=row["order_date"],
            coupon=coupon,
            coupon_code=row["coupon_code"],
            partner=partner,
            partner_name=row.get("partner_name", ""),
            user_type=user_type,
            bracket=bracket_str,
            bracket_payout_usd=Decimal(str(bracket_payout)),
            order_value_usd=Decimal(str(row.get("order_value_usd", 0))),
            revenue_usd=Decimal(str(row.get("revenue_usd", 0))),
            payout_usd=Decimal(str(row.get("payout_usd", 0))),
            profit_usd=Decimal(str(row.get("profit_usd", 0))),
        )
        records.append(record)
    
    # Bulk insert
    NoonEgyptTransaction.objects.bulk_create(records, batch_size=500)
    print(f"✅ Saved {len(records)} Noon Egypt transactions")
    
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
            (egypt_clean["order_date"] >= pd.Timestamp(date_from)) & 
            (egypt_clean["order_date"] <= pd.Timestamp(date_to))
        ].copy()
        print(f"📊 Filtered to {len(egypt_clean)} Egypt rows in date range")
        
        if len(egypt_clean) > 0:
            # Add coupon column for enrichment (enrich_df expects "coupon")
            egypt_clean["coupon"] = egypt_clean["coupon_code"]
            
            # Enrich with partners
            egypt_enriched = enrich_df(egypt_clean, advertiser=advertiser)
            
            # Save (financials already calculated in clean_noon_egypt)
            egypt_count = save_transactions(advertiser, egypt_enriched, date_from, date_to, "egypt")
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
    
    qs = NoonEgyptTransaction.objects.filter(
        order_date__gte=date_from,
        order_date__lte=date_to
    )
    
    if not qs.exists():
        print("⚠️ No NoonEgyptTransaction rows found")
        return 0
    
    # Group by date, partner, coupon
    groups = {}
    
    for r in qs:
        key = (r.order_date, r.partner_name, r.coupon_code)
        
        if key not in groups:
            groups[key] = {
                "date": r.order_date,
                "partner": r.partner,
                "partner_name": r.partner_name,
                "coupon": r.coupon,
                "coupon_code": r.coupon_code,
                "geo": "EGY",
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
        
        # Aggregate by user type
        if r.user_type == "ftu":
            g["ftu_orders"] += 1
            g["ftu_sales"] += float(r.order_value_usd)
            g["ftu_revenue"] += float(r.revenue_usd)
            g["ftu_payout"] += float(r.payout_usd)
        elif r.user_type == "rtu":
            g["rtu_orders"] += 1
            g["rtu_sales"] += float(r.order_value_usd)
            g["rtu_revenue"] += float(r.revenue_usd)
            g["rtu_payout"] += float(r.payout_usd)
    
    # Delete existing performance records for Egypt
    deleted = CampaignPerformance.objects.filter(
        advertiser=advertiser,
        date__gte=date_from,
        date__lte=date_to,
        geo="EGY"
    ).delete()
    print(f"🗑️  Deleted {deleted[0]} existing CampaignPerformance rows for Noon Egypt")
    
    # Create new performance records
    records = []
    for key, g in groups.items():
        # Skip records with blank coupon
        if not g["coupon"]:
            continue
            
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
