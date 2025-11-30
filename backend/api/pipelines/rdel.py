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
    Partner,
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
        # Keep 'coupon' as-is for compatibility with enrich_df helper
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
    df["coupon"] = df["coupon"].str.upper()
    
    # Add standard fields (currency will be set from advertiser later)
    df["rate_type"] = "percent"
    df["commission"] = 0.0  # Not provided in CSV
    
    # Create synthetic order_id (since each row is aggregated)
    df["order_id"] = df.apply(
        lambda row: f"{row['advertiser']}_{row['created_at'].strftime('%Y%m%d')}_{row['coupon']}_{row['country']}",
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
        
        # Ensure advertiser_id and currency are set from advertiser
        enriched["advertiser_id"] = advertiser.id
        enriched["advertiser_name"] = advertiser.name
        enriched["currency"] = advertiser.currency
        
        print(f"✅ Enriched {len(enriched)} rows for {adv_name}")
        
        # Apply payout rules
        payout_df = resolve_payouts_with_history(advertiser, enriched, timestamp_col="created_date")
        
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
                coupon=row.get("coupon", ""),
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
                currency=row.get("currency", "AED"),  # Fallback to AED if not set
            )
        )
    
    RDELTransaction.objects.bulk_create(transactions, batch_size=500)
    print(f"✅ RDEL pipeline inserted {len(transactions)} rows.")
    
    # Now aggregate to CampaignPerformance using same pattern as Styli
    push_rdel_to_performance(start_dt.date(), end_dt.date())


def push_rdel_to_performance(date_from, date_to):
    """
    Aggregate RDEL transactions into CampaignPerformance.
    Follows the same pattern as Styli pipeline.
    """
    qs = RDELTransaction.objects.filter(
        created_date__date__gte=date_from,
        created_date__date__lte=date_to
    )

    if not qs.exists():
        print("⚠️ No RDELTransaction rows found")
        return 0

    groups = {}

    for r in qs:
        key = (
            r.created_date.date(),
            r.advertiser_name,
            r.partner_name,
            r.coupon,
            r.country
        )

        if key not in groups:
            groups[key] = {
                "date": r.created_date.date(),
                "advertiser_name": r.advertiser_name,
                "partner_name": r.partner_name,
                "coupon": r.coupon,
                "geo": r.country,

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
        # Get advertiser for exchange rate on sales only (our_rev/payout already in USD from compute_final_metrics)
        advertiser = Advertiser.objects.filter(name=r.advertiser_name).first()
        exchange_rate = float(advertiser.exchange_rate or 1.0) if advertiser else 1.0

        # RDEL data is all RTU
        # Note: our_rev_usd and payout_usd are already converted by compute_final_metrics()
        if r.user_type == "RTU":
            g["rtu_orders"] += r.order_count
            g["rtu_sales"] += float(r.sales) * exchange_rate  # Sales in AED, convert to USD
            g["rtu_revenue"] += float(r.our_rev_usd)  # Already in USD
            g["rtu_payout"] += float(r.payout_usd)  # Already in USD

    # SAVE to CampaignPerformance
    from django.db import transaction as db_transaction
    from api.models import Coupon
    
    with db_transaction.atomic():
        # Delete existing data for these advertisers in date range
        CampaignPerformance.objects.filter(
            advertiser__name__in=list(ADVERTISER_NAMES),
            date__gte=date_from,
            date__lte=date_to
        ).delete()

        objs = []
        for key, g in groups.items():
            advertiser = Advertiser.objects.filter(name=g["advertiser_name"]).first()
            partner = Partner.objects.filter(name=g["partner_name"]).first() if g["partner_name"] and g["partner_name"] != "(No Partner)" else None
            coupon_obj = Coupon.objects.filter(code=g["coupon"], advertiser=advertiser).first() if g["coupon"] else None

            objs.append(
                CampaignPerformance(
                    date=g["date"],
                    advertiser=advertiser,
                    partner=partner,
                    coupon=coupon_obj,
                    geo=g["geo"],

                    ftu_orders=g["ftu_orders"],
                    rtu_orders=g["rtu_orders"],
                    total_orders=g["ftu_orders"] + g["rtu_orders"],

                    ftu_sales=g["ftu_sales"],
                    rtu_sales=g["rtu_sales"],
                    total_sales=g["ftu_sales"] + g["rtu_sales"],

                    ftu_revenue=g["ftu_revenue"],
                    rtu_revenue=g["rtu_revenue"],
                    total_revenue=g["ftu_revenue"] + g["rtu_revenue"],

                    ftu_payout=g["ftu_payout"],
                    rtu_payout=g["rtu_payout"],
                    total_payout=g["ftu_payout"] + g["rtu_payout"],
                )
            )

        CampaignPerformance.objects.bulk_create(objs, batch_size=2000)

    print(f"✅ Aggregated {len(objs)} performance rows.")
    return len(objs)
