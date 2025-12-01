# backend/api/pipelines/styli.py

import pandas as pd
from datetime import date, datetime
from django.db import transaction
from django.conf import settings

from api.models import (
    Advertiser,
    StyliTransaction,
    CampaignPerformance,
    Partner,
    Coupon,
)

from api.pipelines.helpers import (
    store_raw_snapshot,
    enrich_df,
    resolve_payouts_with_history,
    compute_final_metrics,
    nf,
    nz,
)
from api.services.s3_service import s3_service

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------

ADVERTISER_NAME = "Styli"         # must match Advertiser.name
S3_CSV_KEY = settings.S3_PIPELINE_FILES["styli"]  # From settings.S3_PIPELINE_FILES


# ---------------------------------------------------
# MAIN RUN FUNCTION
# ---------------------------------------------------

def run(date_from: date, date_to: date):
    advertiser = Advertiser.objects.get(name=ADVERTISER_NAME)

    print(f"🚀 Running Styli pipeline {date_from} → {date_to}")

    # 1. LOAD RAW CSV
    raw_df = fetch_raw_data()
    print("🔍 RAW DF HEAD:")
    print(raw_df.head(10))

    # 2. STORE RAW SNAPSHOT (useful for auditing)
    store_raw_snapshot(advertiser, raw_df, date_from, date_to, source="styli_csv_manual")

    # 3. CLEAN
    clean_df = clean_styli(raw_df, advertiser)
    print("🔍 CLEAN DF HEAD:")
    print(clean_df.head(10))

    # 4. ENRICH (coupons → partner, advertiser)
    enriched_df = enrich_df(clean_df, advertiser=advertiser)
    print("🔍 ENRICHED DF HEAD:")
    print(enriched_df.head(10))

    # 5. RESOLVE PAYOUT RULES (ftu/rtu rate)
    payout_df = resolve_payouts_with_history(advertiser, enriched_df, timestamp_col="created_date")
    print("🔍 PAYOUT DF HEAD:")
    print(payout_df.head(10))

    # 6. FINAL METRICS (payout, profit, USD conversion)
    final_df = compute_final_metrics(payout_df, advertiser)
    print("🔍 FINAL DF HEAD:")
    print(final_df.head(10))

    # 7. SAVE INTO StyliTransaction
    count = save_final_rows(advertiser, final_df, date_from, date_to)

    # 8. PUSH TO CAMPAIGN PERFORMANCE
    push_styli_to_performance(date_from, date_to)

    print(f"✅ Styli pipeline inserted {count} rows.")
    return count


# ---------------------------------------------------
# FETCH RAW
# ---------------------------------------------------

def fetch_raw_data() -> pd.DataFrame:
    print("📄 Loading Styli CSV from S3...")
    df = s3_service.read_csv_to_df(S3_CSV_KEY)
    print(f"✅ Loaded {len(df)} rows from S3")
    return df


# ---------------------------------------------------
# CLEANING / NORMALIZATION
# ---------------------------------------------------

def clean_styli(df: pd.DataFrame, advertiser: Advertiser) -> pd.DataFrame:
    print("🧹 Cleaning Styli CSV...")

    df = df.rename(columns={
        "Order Date": "created_at",
        "customer_flag": "user_type",
        "Order Id": "order_id",
        "Coupon": "coupon",
        "country": "country",
        "Order Value (AED)": "sales",
        "Payout (AED)": "commission_value",
    })

    # Standardize user_type
    df["user_type"] = df["user_type"].astype(str).str.upper().str.strip()

    df["user_type"] = df["user_type"].replace({
        "NEW ORDERS": "FTU",
        "EXISTING ORDERS": "RTU",
    }).fillna("FTU")

    # Orders
    df["ftu_orders"] = (df["user_type"] == "FTU").astype(int)
    df["rtu_orders"] = (df["user_type"] == "RTU").astype(int)
    df["orders"] = 1

    # Conversion to numeric
    df["sales"] = pd.to_numeric(df["sales"], errors="coerce").fillna(0.0)
    df["commission"] = pd.to_numeric(df["commission_value"], errors="coerce").fillna(0.0)

    # Date
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    # Country uppercase
    df["country"] = df["country"].astype(str).str.upper()

    # Delivery always delivered for Styli
    df["delivery_status"] = "delivered"

    # Defaults needed for enrichment
    df["partner_id"] = pd.NA
    df["partner_name"] = None
    df["partner_type"] = None
    df["advertiser_id"] = advertiser.id
    df["advertiser_name"] = advertiser.name

   # ✅ NEW: pull from Advertiser model
    df["currency"] = advertiser.currency
    df["rate_type"] = advertiser.rev_rate_type

    return df


# ---------------------------------------------------
# SAVE FINAL ROWS
# ---------------------------------------------------

def save_final_rows(advertiser: Advertiser, df: pd.DataFrame, date_from: date, date_to: date) -> int:

    if df.empty:
        StyliTransaction.objects.filter(
            created_date__gte=date_from,
            created_date__lte=date_to
        ).delete()
        return 0

    with transaction.atomic():
        StyliTransaction.objects.filter(
            created_date__gte=date_from,
            created_date__lte=date_to
        ).delete()

        objs = []
        for r in df.to_dict(orient="records"):
            objs.append(
                StyliTransaction(
                    order_id=r["order_id"],
                    created_date=r.get("created_at"),
                    delivery_status="delivered",
                    country=r.get("country"),
                    coupon=r.get("coupon"),
                    user_type=r.get("user_type"),
                    partner_name=r.get("partner_name"),
                    partner_type=r.get("partner_type"),
                    advertiser_name=r.get("advertiser_name"),
                    currency=advertiser.currency,
                    rate_type=advertiser.rev_rate_type,

                    sales=nf(r.get("sales")),
                    commission=nf(r.get("commission")),
                    our_rev=nf(r.get("our_rev")),

                    ftu_orders=nz(r.get("ftu_orders")),
                    rtu_orders=nz(r.get("rtu_orders")),
                    orders=nz(r.get("orders")),

                    ftu_rate=nf(r.get("ftu_rate")),
                    rtu_rate=nf(r.get("rtu_rate")),

                    payout=nf(r.get("payout")),
                    profit=nf(r.get("profit")),
                    payout_usd=nf(r.get("payout_usd")),
                    profit_usd=nf(r.get("profit_usd")),
                )
            )

        StyliTransaction.objects.bulk_create(objs, batch_size=2000)

    return len(df)


# ---------------------------------------------------
# PUSH TO PERFORMANCE
# ---------------------------------------------------

def push_styli_to_performance(date_from, date_to):
    advertiser = Advertiser.objects.filter(name=ADVERTISER_NAME).first()
    qs = StyliTransaction.objects.filter(
        created_date__date__gte=date_from,
        created_date__date__lte=date_to
    )

    if not qs.exists():
        print("⚠️ No StyliTransaction rows found")
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
        # Use advertiser's exchange rate for USD conversion
        exchange_rate = float(advertiser.exchange_rate or 1.0)

        if r.user_type == "FTU":
            g["ftu_orders"] += r.orders
            g["ftu_sales"] += float(r.sales) * exchange_rate
            g["ftu_revenue"] += float(r.our_rev) * exchange_rate
            g["ftu_payout"] += float(r.payout) * exchange_rate

        elif r.user_type == "RTU":
            g["rtu_orders"] += r.orders
            g["rtu_sales"] += float(r.sales) * exchange_rate
            g["rtu_revenue"] += float(r.our_rev) * exchange_rate
            g["rtu_payout"] += float(r.payout) * exchange_rate

    # SAVE to CampaignPerformance
    with transaction.atomic():
        CampaignPerformance.objects.filter(
            advertiser=advertiser,
            date__gte=date_from,
            date__lte=date_to
        ).delete()

        objs = []
        for key, g in groups.items():
            advertiser = Advertiser.objects.filter(name=g["advertiser_name"]).first()
            partner = Partner.objects.filter(name=g["partner_name"]).first() if g["partner_name"] else None
            coupon_obj = Coupon.objects.filter(code=g["coupon"]).first()

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