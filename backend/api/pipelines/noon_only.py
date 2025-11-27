# backend/api/pipelines/noon_only.py

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
S3_GCC_KEY = "pipeline-data/noon_gcc.csv"
S3_EGYPT_KEY = "pipeline-data/noon_egypt.csv"

# Date cutoff for bracket-based logic
BRACKET_START_DATE = datetime(2025, 11, 18).date()

# Exchange rate for AED to USD (for pre-Nov 18 calculations)
AED_TO_USD = 0.27

# GCC Default Payouts (Post-Nov 18, Bracket-based)
GCC_DEFAULT_PAYOUTS = {
    "$1 Tier": 0.8,      # Bracket 1: <100 AED
    "$2 Tier": 1.6,      # Bracket 2: <150 AED
    "$3.5 Tier": 2.8,    # Bracket 3: <200 AED
    "$5 Tier": 4.0,      # Bracket 4: <400 AED
    "$7.5 Tier": 6.0,    # Bracket 5: >=400 AED
}

# GCC Special Payouts for Haron Ali & Mahmoud Houtan (Post-Nov 18 only)
NOON_GCC_SPECIAL_PAYOUTS = {
    "Haron Ali": {
        "$1 Tier": 0.95,
        "$2 Tier": 1.9,
        "$3.5 Tier": 3.25,
        "$5 Tier": 4.75,
        "$7.5 Tier": 7.0,
    },
    "Mahmoud Houtan": {
        "$1 Tier": 0.95,
        "$2 Tier": 1.9,
        "$3.5 Tier": 3.25,
        "$5 Tier": 4.75,
        "$7.5 Tier": 7.0,
    }
}

# Egypt Default Payouts (Post-Nov 18, Bracket-based)
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

def extract_tier_revenue(tier_str):
    """
    Extract revenue from GCC tier string.
    Examples: "$1 Tier" → 1.0, "$3.5 Tier" → 3.5, "$7.5 Tier" → 7.5
    """
    if pd.isna(tier_str) or not tier_str:
        return 0.0
    
    try:
        # Remove "$" and " Tier", convert to float
        value = tier_str.replace("$", "").replace(" Tier", "").strip()
        return float(value)
    except:
        return 0.0


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


def get_special_payout(partner_name, tier_str, is_gcc, order_date):
    """
    Check if partner gets special payout for this tier.
    Returns special payout amount or None if no special applies.
    """
    # No specials before Nov 18
    if order_date < BRACKET_START_DATE:
        return None
    
    # No specials for Egypt (currently)
    if not is_gcc:
        return None
    
    # Check if partner has special rates
    if partner_name not in NOON_GCC_SPECIAL_PAYOUTS:
        return None
    
    # Get special payout for this tier
    return NOON_GCC_SPECIAL_PAYOUTS[partner_name].get(tier_str)


def calculate_pre_nov18_revenue(ftu_value, rtu_value, currency="AED"):
    """
    Calculate revenue for pre-Nov 18 orders (percentage-based).
    FTU: 3% of sales + 3 AED
    RTU: 3% of sales
    """
    ftu_value = float(ftu_value or 0)
    rtu_value = float(rtu_value or 0)
    
    # Revenue calculation
    revenue_ftu = (ftu_value * 0.03) + (3 * AED_TO_USD if currency == "AED" else 0)
    revenue_rtu = rtu_value * 0.03
    
    total_revenue = revenue_ftu + revenue_rtu
    return round(total_revenue, 2)


def calculate_pre_nov18_payout(ftu_value, rtu_value, currency="AED"):
    """
    Calculate payout for pre-Nov 18 orders (percentage-based).
    FTU: 57.14% of revenue + 2 AED
    RTU: 60% of revenue
    """
    ftu_value = float(ftu_value or 0)
    rtu_value = float(rtu_value or 0)
    
    # First calculate revenue
    revenue_ftu = (ftu_value * 0.03) + (3 * AED_TO_USD if currency == "AED" else 0)
    revenue_rtu = rtu_value * 0.03
    
    # Then calculate payout
    payout_ftu = (revenue_ftu * 0.5714) + (2 * AED_TO_USD if currency == "AED" else 0)
    payout_rtu = revenue_rtu * 0.60
    
    total_payout = payout_ftu + payout_rtu
    return round(total_payout, 2)


# ---------------------------------------------------
# CLEANING / NORMALIZATION
# ---------------------------------------------------

def clean_noon_gcc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean GCC CSV data.
    Expected columns: ORDER DATE, ADVERTISER, PLATFORM, COUNTRY, COUPON CODE, 
                     TIER, TOTAL ORDERS, NON-PAYABLE ORDERS, TOTAL VALUE,
                     FTU ORDERS, FTU ORDER VALUE, RTU ORDERS, RTU ORDER VALUE
    """
    print("🧹 Cleaning Noon GCC data...")
    
    df = df.rename(columns={
        "ORDER DATE": "order_date",
        "ADVERTISER": "advertiser",
        "PLATFORM": "platform",
        "COUNTRY": "country",
        "COUPON CODE": "coupon_code",
        "TIER": "tier",
        "TOTAL ORDERS": "total_orders",
        "NON-PAYABLE ORDERS": "non_payable_orders",
        "TOTAL VALUE": "total_value",
        "FTU ORDERS": "ftu_orders",
        "FTU ORDER VALUE": "ftu_value",
        "RTU ORDERS": "rtu_orders",
        "RTU ORDER VALUE": "rtu_value",
    })
    
    # Parse date
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce").dt.date
    
    # Numeric conversions
    df["total_orders"] = pd.to_numeric(df["total_orders"], errors="coerce").fillna(0).astype(int)
    df["non_payable_orders"] = pd.to_numeric(df["non_payable_orders"], errors="coerce").fillna(0).astype(int)
    df["payable_orders"] = df["total_orders"] - df["non_payable_orders"]
    
    df["total_value"] = pd.to_numeric(df["total_value"], errors="coerce").fillna(0)
    df["ftu_orders"] = pd.to_numeric(df["ftu_orders"], errors="coerce").fillna(0).astype(int)
    df["ftu_value"] = pd.to_numeric(df["ftu_value"], errors="coerce").fillna(0)
    df["rtu_orders"] = pd.to_numeric(df["rtu_orders"], errors="coerce").fillna(0).astype(int)
    df["rtu_value"] = pd.to_numeric(df["rtu_value"], errors="coerce").fillna(0)
    
    # String fields
    df["coupon_code"] = df["coupon_code"].astype(str).str.strip()
    df["tier"] = df["tier"].astype(str).str.strip()
    df["platform"] = df["platform"].astype(str).str.strip()
    df["country"] = df["country"].astype(str).str.strip()
    
    # Add region flags
    df["is_gcc"] = True
    df["region"] = "gcc"
    
    # Filter out zero orders
    df = df[df["payable_orders"] > 0].copy()
    
    print(f"✅ Cleaned {len(df)} GCC rows")
    return df


def clean_noon_egypt(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean Egypt CSV data.
    Expected columns: order_date, platform, country, coupon_code, 
                     bracket columns (Bracket X_$Y.YY), order_value_gmv_usd,
                     FTU/RTU order counts and values
    """
    print("🧹 Cleaning Noon Egypt data...")
    
    # Identify bracket columns
    bracket_cols = [col for col in df.columns if col.startswith("Bracket") and "_$" in col]
    
    # For now, use the first bracket column found (we'll enhance this if needed)
    if not bracket_cols:
        print("⚠️  No bracket columns found in Egypt data")
        return pd.DataFrame()
    
    # Rename standard columns
    df = df.rename(columns={
        "order_date": "order_date",
        "platform": "platform",
        "country": "country",
        "coupon_code": "coupon_code",
        "order_value_gmv_usd": "total_value",
        "FTU ORDERS": "ftu_orders",
        "FTU ORDER VALUE": "ftu_value",
        "RTU ORDERS": "rtu_orders",
        "RTU ORDER VALUE": "rtu_value",
    })
    
    # Parse date
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce").dt.date
    
    # Find which bracket has data (sum all bracket columns)
    df["total_orders"] = df[bracket_cols].sum(axis=1)
    df["non_payable_orders"] = 0  # Egypt data doesn't have this field
    df["payable_orders"] = df["total_orders"]
    
    # Determine active bracket per row (take first non-zero bracket)
    df["tier"] = ""
    for col in bracket_cols:
        mask = (df["tier"] == "") & (df[col] > 0)
        df.loc[mask, "tier"] = col
    
    # Numeric conversions
    df["total_value"] = pd.to_numeric(df["total_value"], errors="coerce").fillna(0)
    
    if "ftu_orders" in df.columns:
        df["ftu_orders"] = pd.to_numeric(df["ftu_orders"], errors="coerce").fillna(0).astype(int)
    else:
        df["ftu_orders"] = 0
        
    if "ftu_value" in df.columns:
        df["ftu_value"] = pd.to_numeric(df["ftu_value"], errors="coerce").fillna(0)
    else:
        df["ftu_value"] = 0
        
    if "rtu_orders" in df.columns:
        df["rtu_orders"] = pd.to_numeric(df["rtu_orders"], errors="coerce").fillna(0).astype(int)
    else:
        df["rtu_orders"] = 0
        
    if "rtu_value" in df.columns:
        df["rtu_value"] = pd.to_numeric(df["rtu_value"], errors="coerce").fillna(0)
    else:
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
        
        # Pre-Nov 18: Percentage-based
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
            # Post-Nov 18: Bracket-based
            if is_gcc:
                # Extract revenue from tier
                revenue_usd = extract_tier_revenue(tier)
                
                # Check for special payout
                special_payout = get_special_payout(partner_name, tier, is_gcc, order_date)
                if special_payout:
                    payout_usd = special_payout
                else:
                    payout_usd = GCC_DEFAULT_PAYOUTS.get(tier, 0)
            else:
                # Egypt: Extract revenue from bracket
                revenue_usd = extract_bracket_revenue(tier)
                
                # Get default payout based on bracket number
                bracket_num = extract_bracket_number(tier)
                payout_usd = EGYPT_DEFAULT_PAYOUTS.get(bracket_num, 0) if bracket_num else 0
        
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

def save_transactions(advertiser: Advertiser, df: pd.DataFrame, date_from: date, date_to: date) -> int:
    """
    Save processed transactions to NoonTransaction table.
    """
    print(f"💾 Saving Noon transactions...")
    
    # Delete existing records in date range
    deleted_count, _ = NoonTransaction.objects.filter(
        order_date__gte=date_from,
        order_date__lte=date_to,
    ).delete()
    
    if deleted_count > 0:
        print(f"🗑️  Deleted {deleted_count} existing records")
    
    # Prepare records for bulk insert
    records = []
    for idx, row in df.iterrows():
        # Get coupon object
        coupon = None
        if row.get("coupon_id"):
            try:
                coupon = Coupon.objects.get(id=row["coupon_id"])
            except Coupon.DoesNotExist:
                pass
        
        # Get partner object
        partner = None
        if row.get("partner_id"):
            try:
                partner = Partner.objects.get(id=row["partner_id"])
            except Partner.DoesNotExist:
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
    Processes both GCC and Egypt data.
    """
    advertiser = Advertiser.objects.get(name=ADVERTISER_NAME)
    
    print(f"🚀 Running Noon pipeline {date_from} → {date_to}")
    print(f"📅 Bracket logic applies from: {BRACKET_START_DATE}")
    
    total_count = 0
    
    # ---------------------------------------------------
    # PROCESS GCC DATA
    # ---------------------------------------------------
    try:
        print("\n" + "="*60)
        print("📍 PROCESSING NOON GCC DATA")
        print("="*60)
        
        # Load from S3
        print(f"📄 Loading GCC CSV from S3: {S3_GCC_KEY}")
        gcc_df = s3_service.read_csv_to_df(S3_GCC_KEY)
        print(f"✅ Loaded {len(gcc_df)} GCC rows")
        
        # Clean
        gcc_clean = clean_noon_gcc(gcc_df)
        
        # Filter date range
        gcc_clean = gcc_clean[
            (gcc_clean["order_date"] >= date_from) & 
            (gcc_clean["order_date"] <= date_to)
        ].copy()
        print(f"📊 Filtered to {len(gcc_clean)} GCC rows in date range")
        
        if len(gcc_clean) > 0:
            # Enrich with partners
            gcc_enriched = enrich_df(gcc_clean, advertiser=advertiser)
            
            # Calculate financials
            gcc_final = calculate_financials(gcc_enriched, advertiser)
            
            # Save
            gcc_count = save_transactions(advertiser, gcc_final, date_from, date_to)
            total_count += gcc_count
            print(f"✅ GCC: {gcc_count} transactions saved")
        else:
            print("⚠️  No GCC data in date range")
    
    except Exception as e:
        print(f"⚠️  GCC processing failed: {e}")
        import traceback
        traceback.print_exc()
    
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
            # Enrich with partners
            egypt_enriched = enrich_df(egypt_clean, advertiser=advertiser)
            
            # Calculate financials
            egypt_final = calculate_financials(egypt_enriched, advertiser)
            
            # Save
            egypt_count = save_transactions(advertiser, egypt_final, date_from, date_to)
            total_count += egypt_count
            print(f"✅ Egypt: {egypt_count} transactions saved")
        else:
            print("⚠️  No Egypt data in date range")
    
    except Exception as e:
        print(f"⚠️  Egypt processing failed: {e}")
        import traceback
        traceback.print_exc()
    
    # ---------------------------------------------------
    # SUMMARY
    # ---------------------------------------------------
    print("\n" + "="*60)
    print(f"✅ Noon pipeline completed: {total_count} total transactions")
    print("="*60)
    
    return total_count
