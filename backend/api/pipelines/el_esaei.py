# backend/api/pipelines/el_esaei.py

import pandas as pd
from datetime import date, datetime
from django.db import transaction
from django.conf import settings

from api.models import (
    Advertiser,
    RDELTransaction,
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
from api.pipelines.rdel_shared import clean_rdel_format
from api.services.s3_service import s3_service

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------

ADVERTISER_NAME = "El_Esaei_Kids"
S3_CSV_KEY = "pipeline-data/el_esaei_kids.csv"


# ---------------------------------------------------
# MAIN RUN FUNCTION
# ---------------------------------------------------

def run(date_from: date, date_to: date):
    advertiser = Advertiser.objects.get(name=ADVERTISER_NAME)

    print(f"🚀 Running El_Esaei_Kids pipeline {date_from} → {date_to}")

    # 1. LOAD RAW CSV
    raw_df = fetch_raw_data()
    print("🔍 RAW DF HEAD:")
    print(raw_df.head(10))

    # 2. STORE RAW SNAPSHOT
    store_raw_snapshot(advertiser, raw_df, date_from, date_to, source="el_esaei_csv_sheet")

    # 3. CLEAN (using shared RDEL format cleaner)
    clean_df = clean_rdel_format(raw_df, advertiser)
    print("🔍 CLEAN DF HEAD:")
    print(clean_df.head(10))

    # 4. ENRICH (coupons → partner, advertiser)
    enriched_df = enrich_df(clean_df, advertiser=advertiser)
    print("🔍 ENRICHED DF HEAD:")
    print(enriched_df.head(10))

    # 5. RESOLVE PAYOUT RULES
    payout_df = resolve_payouts_with_history(advertiser, enriched_df)
    print("🔍 PAYOUT DF HEAD:")
    print(payout_df.head(10))

    # 6. FINAL METRICS
    final_df = compute_final_metrics(payout_df, advertiser)
    print("🔍 FINAL DF HEAD:")
    print(final_df.head(10))

    # 7. SAVE INTO RDELTransaction
    count = save_final_rows(advertiser, final_df, date_from, date_to)

    # 8. PUSH TO CAMPAIGN PERFORMANCE
    push_el_esaei_to_performance(date_from, date_to)

    print(f"✅ El_Esaei_Kids pipeline inserted {count} rows.")
    return count


# ---------------------------------------------------
# FETCH RAW
# ---------------------------------------------------

def fetch_raw_data() -> pd.DataFrame:
    print("📄 Loading El_Esaei_Kids CSV from S3...")
    df = s3_service.read_csv_to_df(S3_CSV_KEY)
    print(f"✅ Loaded {len(df)} rows from S3")
    return df


# ---------------------------------------------------
# SAVE FINAL ROWS
# ---------------------------------------------------

def save_final_rows(advertiser: Advertiser, df: pd.DataFrame, date_from: date, date_to: date) -> int:
    if df.empty:
        RDELTransaction.objects.filter(
            advertiser_name=ADVERTISER_NAME,
            created_date__date__gte=date_from,
            created_date__date__lte=date_to
        ).delete()
        return 0

    with transaction.atomic():
        RDELTransaction.objects.filter(
            advertiser_name=ADVERTISER_NAME,
            created_date__date__gte=date_from,
            created_date__date__lte=date_to
        ).delete()

        objs = []
        for r in df.to_dict(orient="records"):
            partner_id = r.get("partner_id")
            if pd.isna(partner_id):
                partner_id = None
            
            advertiser_id = r.get("advertiser_id")
            if pd.isna(advertiser_id):
                advertiser_id = None
                
            objs.append(
                RDELTransaction(
                    order_id=str(r["order_id"]),
                    created_date=r.get("created_at"),
                    user_type=r.get("user_type"),
                    sales=nf(r.get("sales")),
                    commission=nf(r.get("commission")),
                    country=r.get("country", ""),
                    order_count=nz(r.get("order_count", 1)),
                    coupon=r.get("coupon", ""),
                    partner_id=partner_id,
                    partner_name=r.get("partner_name", "(No Partner)"),
                    advertiser_id=advertiser_id,
                    advertiser_name=ADVERTISER_NAME,
                    ftu_rate=nf(r.get("ftu_rate")),
                    rtu_rate=nf(r.get("rtu_rate")),
                    rate_type=r.get("rate_type", "percent"),
                    ftu_fixed_bonus=nf(r.get("ftu_fixed_bonus")),
                    rtu_fixed_bonus=nf(r.get("rtu_fixed_bonus")),
                    payout=nf(r.get("payout")),
                    our_rev=nf(r.get("our_rev")),
                    our_rev_usd=nf(r.get("our_rev_usd")),
                    payout_usd=nf(r.get("payout_usd")),
                    profit_usd=nf(r.get("profit_usd")),
                    currency=r.get("currency", advertiser.currency),
                )
            )

        RDELTransaction.objects.bulk_create(objs, batch_size=500)

    return len(df)


# ---------------------------------------------------
# PUSH TO PERFORMANCE
# ---------------------------------------------------

def push_el_esaei_to_performance(date_from, date_to):
    advertiser = Advertiser.objects.filter(name=ADVERTISER_NAME).first()
    qs = RDELTransaction.objects.filter(
        advertiser_name=ADVERTISER_NAME,
        created_date__date__gte=date_from,
        created_date__date__lte=date_to
    )

    if not qs.exists():
        print("⚠️ No El_Esaei_Kids transactions found")
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
        exchange_rate = float(advertiser.exchange_rate or 1.0) if advertiser else 1.0

        if r.user_type == "FTU":
            g["ftu_orders"] += r.order_count
            g["ftu_sales"] += float(r.sales) * exchange_rate
            g["ftu_revenue"] += float(r.our_rev_usd)
            g["ftu_payout"] += float(r.payout_usd)
        elif r.user_type == "RTU":
            g["rtu_orders"] += r.order_count
            g["rtu_sales"] += float(r.sales) * exchange_rate
            g["rtu_revenue"] += float(r.our_rev_usd)
            g["rtu_payout"] += float(r.payout_usd)

    # SAVE to CampaignPerformance
    with transaction.atomic():
        CampaignPerformance.objects.filter(
            advertiser=advertiser,
            date__gte=date_from,
            date__lte=date_to
        ).delete()

        objs = []
        for key, g in groups.items():
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

    print(f"✅ Aggregated {len(objs)} El_Esaei_Kids performance rows.")
    return len(objs)
