"""
Reef, Daham, El_Esaei_Kids Pipeline
Processes Google Sheet data for three advertisers together.
"""

import pandas as pd
from datetime import datetime
from django.utils import timezone
from django.conf import settings
from api.models import (
    Advertiser,
    RDELTransaction,
    CampaignPerformance,
)
from .helpers import enrich_df, resolve_payouts, compute_final_metrics, store_raw_snapshot
from api.services.s3_service import s3_service


# Advertiser names as they appear in the CSV
ADVERTISER_NAMES = {"Reef", "Daham", "El_Esaei_Kids"}


def clean_rdel_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform RDEL CSV data.
    
    Expected columns:
    - date: transaction date (M/D/YYYY)
    - advertiser: Reef, Daham, or El_Esaei_Kids
    - coupon: coupon code
    - country: KSA, UAE, KWT, QA, BAH, OMA
    - orders: number of orders
    - sales: sales amount in SAR
    """
    df = df.copy()
    
    # Rename columns to standard format
    df.rename(columns={
        "date": "created_at",
        "coupon": "coupon_code",
        # Keep 'orders' as-is for compatibility with helpers
        "sales": "sales",  # Keep as 'sales' for compatibility with helpers
    }, inplace=True)
    
    # Parse date (M/D/YYYY format)
    df["created_at"] = pd.to_datetime(df["created_at"], format="%m/%d/%Y")
    
    # Clean sales amount - remove commas and handle formatting
    df["sales"] = (
        df["sales"]
        .astype(str)
        .str.replace(",", "")
        .str.replace("%", "")  # Handle edge cases like "241.45%"
        .astype(float)
    )
    
    # Clean order count
    df["orders"] = df["orders"].astype(int)
    
    # Uppercase coupon codes for consistency
    df["coupon_code"] = df["coupon_code"].str.upper()
    
    # Add standard fields
    df["currency"] = "SAR"
    df["rate_type"] = "percent"
    df["commission"] = 0.0  # Not provided in CSV
    
    # Create synthetic order_id (since each row is aggregated)
    df["order_id"] = df.apply(
        lambda row: f"{row['advertiser']}_{row['created_at'].strftime('%Y%m%d')}_{row['coupon_code']}_{row['country']}",
        axis=1
    )
    
    # We don't have user_type (FTU/RTU) in the data, so we'll default to RTU
    # The payout logic will handle this appropriately
    df["user_type"] = "RTU"
    
    # Add order_count as alias for our transaction model (helpers use 'orders')
    df["order_count"] = df["orders"]
    
    print(f"🔍 CLEAN DF HEAD:")
    print(df.head(10))
    
    return df


def run_rdel_pipeline(start_date: str, end_date: str):
    """
    Run the Reef/Daham/El_Esaei_Kids pipeline.
    
    Args:
        start_date: Start date string (YYYY-MM-DD)
        end_date: End date string (YYYY-MM-DD)
    """
    print(f"🚀 Running RDEL pipeline {start_date} → {end_date}")
    
    # Load CSV from S3
    print("📄 Loading RDEL CSV from S3...")
    raw_df = s3_service.read_csv_to_df("pipeline-data/rdel.csv")
    print(f"✅ Loaded {len(raw_df)} rows")
    
    # Clean data
    clean_df = clean_rdel_data(raw_df)
    
    # Filter by date range
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    clean_df = clean_df[
        (clean_df["created_at"] >= start) & (clean_df["created_at"] <= end)
    ]
    print(f"📅 Filtered to {len(clean_df)} rows in date range")
    
    # Process each advertiser separately (to handle duplicate coupons)
    all_enriched = []
    
    for adv_name in ADVERTISER_NAMES:
        # Filter rows for this advertiser
        adv_rows = clean_df[clean_df["advertiser"] == adv_name].copy()
        
        if len(adv_rows) == 0:
            print(f"⚠️  No data for {adv_name}")
            continue
        
        # Get advertiser object
        try:
            advertiser = Advertiser.objects.get(name=adv_name)
        except Advertiser.DoesNotExist:
            print(f"❌ Advertiser '{adv_name}' not found in database. Skipping.")
            continue
        
        print(f"🔍 Resolving coupon ownership by transaction date for {adv_name}...")
        enriched = enrich_df(adv_rows, advertiser=advertiser)
        print(f"✅ Enriched {len(enriched)} rows for {adv_name}")
        
        # Apply payout rules
        payout_df = resolve_payouts(advertiser, enriched)
        
        # Calculate final metrics for this advertiser
        final_adv_df = compute_final_metrics(payout_df, advertiser)
        
        all_enriched.append(final_adv_df)
    
    # Combine all advertisers
    if not all_enriched:
        print("❌ No data to process")
        return
    
    final_df = pd.concat(all_enriched, ignore_index=True)
    
    print(f"🔍 FINAL DF HEAD:")
    print(final_df.head(10))
    
    # Clear existing data in date range
    start_dt = timezone.make_aware(datetime.strptime(start_date, "%Y-%m-%d"))
    end_dt = timezone.make_aware(datetime.strptime(end_date, "%Y-%m-%d"))
    
    RDELTransaction.objects.filter(
        created_date__gte=start_dt,
        created_date__lte=end_dt,
    ).delete()
    
    # Insert transactions
    transactions = []
    for _, row in final_df.iterrows():
        # Handle partner_id - convert pandas NA to None
        partner_id = row.get("partner_id")
        if pd.isna(partner_id):
            partner_id = None
        
        # Handle advertiser_id - convert pandas NA to None
        advertiser_id = row.get("advertiser_id")
        if pd.isna(advertiser_id):
            advertiser_id = None
            
        transactions.append(
            RDELTransaction(
                order_id=str(row["order_id"]),
                created_date=row["created_at"],
                user_type=row["user_type"],
                sales=row.get("sales", 0),
                commission=row.get("commission", 0),
                country=row.get("country", ""),
                order_count=row.get("order_count", 1),
                coupon=row.get("coupon_code", ""),
                partner_id=partner_id,
                partner_name=row.get("partner_name", "(No Partner)"),
                advertiser_id=advertiser_id,
                advertiser_name=row.get("advertiser_name", ""),
                ftu_rate=row.get("ftu_rate", 0),
                rtu_rate=row.get("rtu_rate", 0),
                rate_type=row.get("rate_type", "percent"),
                ftu_fixed_bonus=row.get("ftu_fixed_bonus", 0),
                rtu_fixed_bonus=row.get("rtu_fixed_bonus", 0),
                payout=row.get("payout", 0),
                our_rev=row.get("our_rev", 0),
                our_rev_usd=row.get("our_rev_usd", 0),
                payout_usd=row.get("payout_usd", 0),
                profit_usd=row.get("profit_usd", 0),
                currency=row.get("currency", "SAR"),
            )
        )
    
    RDELTransaction.objects.bulk_create(transactions, batch_size=500)
    print(f"✅ RDEL pipeline inserted {len(transactions)} rows.")
    
    # Aggregate to CampaignPerformance
    perf_data = (
        final_df.groupby(
            ["advertiser_id", "advertiser_name", "partner_id", "partner_name", "created_at"]
        )
        .agg({
            "order_count": "sum",
            "sales": "sum",
            "commission": "sum",
            "our_rev": "sum",
            "payout": "sum",
            "our_rev_usd": "sum",
            "payout_usd": "sum",
            "profit_usd": "sum",
        })
        .reset_index()
    )
    
    # Clear existing performance data
    CampaignPerformance.objects.filter(
        advertiser__name__in=list(ADVERTISER_NAMES),
        date__gte=start_dt.date(),
        date__lte=end_dt.date(),
    ).delete()
    
    # Insert performance rows
    perf_rows = []
    for _, row in perf_data.iterrows():
        perf_rows.append(
            CampaignPerformance(
                advertiser_id=row["advertiser_id"],
                partner_id=row["partner_id"] if pd.notna(row["partner_id"]) else None,
                date=row["created_at"].date(),
                orders=int(row["order_count"]),
                sales=row["sales"],
                commission=row["commission"],
                our_revenue=row["our_rev"],
                payout=row["payout"],
                our_revenue_usd=row["our_rev_usd"],
                payout_usd=row["payout_usd"],
                profit_usd=row["profit_usd"],
            )
        )
    
    CampaignPerformance.objects.bulk_create(perf_rows, batch_size=500)
    print(f"✅ Aggregated {len(perf_rows)} performance rows.")
    
    print(f"✅ Done. Inserted {len(transactions)} final rows.")
