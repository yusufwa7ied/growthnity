# backend/api/pipelines/noon_historical.py
"""
Pipeline for processing historical Noon data (pre-Nov 18, 2025).
Uses the same CSV structure as Namshi but with percentage-based payouts.
"""

import pandas as pd
from datetime import date
from decimal import Decimal
from django.db import transaction
from django.conf import settings

from api.models import (
    Advertiser,
    NoonTransaction,
    CampaignPerformance,
    Partner,
    Coupon,
    PartnerPayout,
)

from api.pipelines.helpers import (
    store_raw_snapshot,
    enrich_df,
    compute_final_metrics,
    nf,
    nz,
)
from api.services.s3_service import s3_service

# S3 key for historical Noon data
S3_CSV_KEY = "pipeline-data/noon_history.csv"

COUNTRY_MAP = {
    "SA": "SAU",
    "AE": "ARE",
    "UAE": "ARE",
    "QA": "QAT",
    "KW": "KWT",
    "OM": "OMN",
    "BH": "BHR",
    "EG": "EGY",
}


def clean_noon_historical(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean historical Noon CSV data (same structure as Namshi).
    
    Input columns:
      Order Date | Advertiser | Country | Coupon Code | Total orders | NON-PAYABLE Orders
      | Total Order Value | FTU Orders | FTU Order Values | RTU Orders | RTU Order Value | Platform
    
    Output: Split FTU/RTU into separate rows with user_type.
    """
    # Normalize column names
    df = df.rename(columns={
        "Order Date": "created_at",
        "Advertiser": "advertiser_name",
        "Country": "country",
        "Coupon Code": "coupon",
        "Total orders": "total_orders",
        "NON-PAYABLE Orders": "nonpayable_orders",
        "Total Order Value": "total_value",
        "FTU Orders": "ftu_orders_src",
        "FTU Order Values": "ftu_value",
        "RTU Orders": "rtu_orders_src",
        "RTU Order Value": "rtu_value",
        "Platform": "platform",
    })

    # Cast and clean
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["advertiser_name"] = df["advertiser_name"].astype(str).str.strip()
    df["coupon"] = df["coupon"].astype(str).str.strip().str.upper()
    df["country"] = df["country"].astype(str).str.upper().replace(COUNTRY_MAP)

    for c in ["total_orders", "nonpayable_orders", "ftu_orders_src", "rtu_orders_src"]:
        df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0).astype(int)

    for c in ["total_value", "ftu_value", "rtu_value"]:
        df[c] = pd.to_numeric(df.get(c, 0.0), errors="coerce").fillna(0.0)

    rows = []

    def emit_row(src, user_type, orders_col, value_col):
        """Emit one row for either FTU or RTU"""
        orders = int(src[orders_col])
        if orders <= 0:
            return
        sales = float(src[value_col])

        rows.append({
            "order_id": 0,
            "created_at": src["created_at"],
            "delivery_status": "delivered",
            "country": src["country"],
            "coupon": src["coupon"],
            "user_type": user_type,
            "partner_id": pd.NA,
            "partner_name": None,
            "partner_type": None,
            "advertiser_name": src["advertiser_name"],
            "advertiser_id": pd.NA,
            "currency": "AED",  # Historical Noon was in AED
            "platform": src.get("platform", ""),
            "orders": orders,
            "sales": sales,
        })

    # Split each row into FTU and RTU
    for idx, row in df.iterrows():
        emit_row(row, "FTU", "ftu_orders_src", "ftu_value")
        emit_row(row, "RTU", "rtu_orders_src", "rtu_value")

    result = pd.DataFrame(rows)
    
    # Drop rows with invalid dates or zero orders
    result = result.dropna(subset=["created_at"])
    result = result[result["orders"] > 0].copy()
    
    print(f"✅ Cleaned {len(result)} rows from historical Noon data")
    return result


def calculate_historical_payout(df: pd.DataFrame, advertiser: Advertiser) -> pd.DataFrame:
    """
    Calculate payout using percentage-based rates from PartnerPayout.
    Similar to Namshi logic but for historical Noon.
    """
    df = df.copy()
    
    # Initialize payout columns
    df["revenue"] = 0.0
    df["payout"] = 0.0
    df["profit"] = 0.0
    df["rate_type"] = ""
    df["ftu_rate"] = 0.0
    df["rtu_rate"] = 0.0
    
    for idx, row in df.iterrows():
        partner_id = row.get("partner_id")
        user_type = row.get("user_type", "")
        sales = float(row.get("sales", 0))
        
        if pd.isna(partner_id) or not partner_id:
            # No partner, no payout
            continue
        
        try:
            partner = Partner.objects.get(id=int(partner_id))
            
            # Get payout configuration
            try:
                config = PartnerPayout.objects.get(advertiser=advertiser, partner=partner)
                ftu_rate = float(config.ftu_payout)
                rtu_rate = float(config.rtu_payout)
                rate_type = config.rate_type or "percent"
            except PartnerPayout.DoesNotExist:
                # Use advertiser defaults
                ftu_rate = float(advertiser.default_ftu_payout)
                rtu_rate = float(advertiser.default_rtu_payout)
                rate_type = "percent"
            
            # Calculate payout based on user type
            if user_type == "FTU":
                rate = ftu_rate
            elif user_type == "RTU":
                rate = rtu_rate
            else:
                rate = 0.0
            
            # Percentage-based calculation
            if rate_type == "percent":
                payout = (sales * rate) / 100.0
            else:
                # Fixed rate (unlikely for historical data)
                payout = rate
            
            # Revenue is same as sales for percentage model
            revenue = sales
            profit = revenue - payout
            
            df.at[idx, "revenue"] = revenue
            df.at[idx, "payout"] = payout
            df.at[idx, "profit"] = profit
            df.at[idx, "rate_type"] = rate_type
            df.at[idx, "ftu_rate"] = ftu_rate
            df.at[idx, "rtu_rate"] = rtu_rate
            
        except (Partner.DoesNotExist, ValueError, TypeError):
            continue
    
    print(f"💰 Calculated payouts for {len(df)} rows")
    return df


def save_historical_transactions(advertiser: Advertiser, df: pd.DataFrame, date_from: date, date_to: date) -> int:
    """
    Save historical Noon transactions to NoonTransaction model.
    Map percentage-based data to the NoonTransaction schema.
    """
    # Delete existing records in date range
    deleted_count, _ = NoonTransaction.objects.filter(
        advertiser_name="Noon",
        order_date__gte=date_from,
        order_date__lte=date_to,
    ).delete()
    
    if deleted_count > 0:
        print(f"🗑️  Deleted {deleted_count} existing historical Noon records")
    
    records = []
    for idx, row in df.iterrows():
        # Get coupon and partner objects
        coupon = None
        coupon_id = row.get("coupon_id")
        if pd.notna(coupon_id) and coupon_id:
            try:
                coupon = Coupon.objects.get(id=int(coupon_id))
            except (Coupon.DoesNotExist, ValueError, TypeError):
                pass
        
        partner = None
        partner_id = row.get("partner_id")
        if pd.notna(partner_id) and partner_id:
            try:
                partner = Partner.objects.get(id=int(partner_id))
            except (Partner.DoesNotExist, ValueError, TypeError):
                pass
        
        order_date = row["created_at"].date() if pd.notna(row["created_at"]) else date_from
        user_type = row.get("user_type", "")
        orders = int(row.get("orders", 0))
        
        # Determine region from country
        country = row.get("country", "")
        if country == "EGY":
            region = "egypt"
            is_gcc = False
        else:
            region = "gcc"
            is_gcc = True
        
        record = NoonTransaction(
            order_id=f"noon_hist_{region}_{order_date}_{row.get('coupon', '')}_{idx}",
            order_date=order_date,
            advertiser_name="Noon",
            is_gcc=is_gcc,
            region=region,
            platform=row.get("platform", ""),
            country=country,
            coupon=coupon,
            coupon_code=row.get("coupon", ""),
            tier_bracket="Historical (Percentage)",  # Mark as historical
            total_orders=orders,
            non_payable_orders=0,  # Not tracked in historical data
            payable_orders=orders,
            total_value=Decimal(str(row.get("sales", 0))),
            ftu_orders=orders if user_type == "FTU" else 0,
            ftu_value=Decimal(str(row.get("sales", 0))) if user_type == "FTU" else Decimal("0"),
            rtu_orders=orders if user_type == "RTU" else 0,
            rtu_value=Decimal(str(row.get("sales", 0))) if user_type == "RTU" else Decimal("0"),
            user_type=user_type,
            partner=partner,
            partner_name=row.get("partner_name", "(No Partner)"),
            revenue_usd=Decimal(str(row.get("revenue", 0))),
            payout_usd=Decimal(str(row.get("payout", 0))),
            our_rev_usd=Decimal(str(row.get("profit", 0))),
        )
        records.append(record)
    
    # Bulk create
    NoonTransaction.objects.bulk_create(records, batch_size=500)
    print(f"✅ Saved {len(records)} historical Noon transactions")
    
    return len(records)


def push_historical_to_performance(date_from: date, date_to: date):
    """
    Aggregate historical Noon transactions into CampaignPerformance table.
    Groups by date, partner, coupon, region (like modern Noon pipeline).
    """
    from django.db.models import Sum, Count
    
    # Get Noon advertiser
    try:
        advertiser = Advertiser.objects.get(name="Noon")
    except Advertiser.DoesNotExist:
        print("⚠️  Noon advertiser not found")
        return
    
    # Delete existing performance rows for this date range
    deleted_count, _ = CampaignPerformance.objects.filter(
        advertiser=advertiser,
        date__gte=date_from,
        date__lte=date_to,
    ).delete()
    
    if deleted_count > 0:
        print(f"🗑️  Deleted {deleted_count} existing CampaignPerformance rows for historical Noon")
    
    # Aggregate transactions
    transactions = NoonTransaction.objects.filter(
        advertiser_name="Noon",
        order_date__gte=date_from,
        order_date__lte=date_to,
        tier_bracket="Historical (Percentage)",  # Only historical data
    ).values(
        "order_date",
        "partner_id",
        "coupon_id",
        "region",
        "user_type",
    ).annotate(
        orders=Sum("payable_orders"),
        sales=Sum("total_value"),
        revenue=Sum("revenue_usd"),
        payout=Sum("payout_usd"),
        our_rev=Sum("our_rev_usd"),
    )
    
    # Group and create performance records
    performance_records = []
    for trans in transactions:
        partner_id = trans["partner_id"]
        coupon_id = trans["coupon_id"]
        
        performance_records.append(CampaignPerformance(
            date=trans["order_date"],
            advertiser=advertiser,
            partner_id=partner_id,
            coupon_id=coupon_id,
            region=trans["region"],
            total_orders=trans["orders"] or 0,
            ftu_orders=trans["orders"] if trans["user_type"] == "FTU" else 0,
            rtu_orders=trans["orders"] if trans["user_type"] == "RTU" else 0,
            total_sales=float(trans["sales"] or 0),
            ftu_sales=float(trans["sales"] or 0) if trans["user_type"] == "FTU" else 0.0,
            rtu_sales=float(trans["sales"] or 0) if trans["user_type"] == "RTU" else 0.0,
            total_revenue=float(trans["revenue"] or 0),
            ftu_revenue=float(trans["revenue"] or 0) if trans["user_type"] == "FTU" else 0.0,
            rtu_revenue=float(trans["revenue"] or 0) if trans["user_type"] == "RTU" else 0.0,
            total_payout=float(trans["payout"] or 0),
            ftu_payout=float(trans["payout"] or 0) if trans["user_type"] == "FTU" else 0.0,
            rtu_payout=float(trans["payout"] or 0) if trans["user_type"] == "RTU" else 0.0,
            total_our_rev=float(trans["our_rev"] or 0),
            ftu_our_rev=float(trans["our_rev"] or 0) if trans["user_type"] == "FTU" else 0.0,
            rtu_our_rev=float(trans["our_rev"] or 0) if trans["user_type"] == "RTU" else 0.0,
        ))
    
    CampaignPerformance.objects.bulk_create(performance_records, batch_size=500)
    print(f"✅ Aggregated {len(performance_records)} performance rows for historical Noon")


@transaction.atomic
def run(date_from: date, date_to: date):
    """
    Main pipeline execution for historical Noon data.
    """
    print(f"🚀 Running Noon HISTORICAL pipeline {date_from} → {date_to}")
    print(f"📄 Using percentage-based payout logic (pre-Nov 18 data)")
    
    # Get Noon advertiser
    try:
        advertiser = Advertiser.objects.get(name="Noon")
    except Advertiser.DoesNotExist:
        print("❌ Noon advertiser not found in database")
        return
    
    # Load CSV from S3
    print(f"📄 Loading historical Noon CSV from S3: {S3_CSV_KEY}")
    raw_df = s3_service.read_csv_to_df(S3_CSV_KEY)
    print(f"✅ Loaded {len(raw_df)} rows from S3")
    
    # Clean and normalize
    clean_df = clean_noon_historical(raw_df)
    
    # Filter to date range and only Noon advertiser
    clean_df = clean_df[
        (clean_df["created_at"].dt.date >= date_from) &
        (clean_df["created_at"].dt.date <= date_to) &
        (clean_df["advertiser_name"].str.upper() == "NOON")
    ].copy()
    
    print(f"📊 Filtered to {len(clean_df)} Noon rows in date range")
    
    if len(clean_df) == 0:
        print("⚠️  No historical Noon data in date range")
        return
    
    # Enrich with partner/coupon data
    enriched_df = enrich_df(clean_df, advertiser=advertiser)
    
    # Calculate payouts using percentage logic
    final_df = calculate_historical_payout(enriched_df, advertiser)
    
    # Save to database
    count = save_historical_transactions(advertiser, final_df, date_from, date_to)
    
    # Aggregate to performance table
    print("\n📊 Aggregating to CampaignPerformance...")
    push_historical_to_performance(date_from, date_to)
    
    print(f"\n✅ Historical Noon pipeline completed: {count} transactions")
