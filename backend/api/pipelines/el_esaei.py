# backend/api/pipelines/el_esaei.py

import pandas as pd
from datetime import date, datetime
from django.db import transaction
from django.conf import settings

from api.models import (
    Advertiser,
    ElEsaeiKidsTransaction,
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

# Country mapping - standardize to 3-letter ISO codes
COUNTRY_MAP = {
    "KSA": "SAU",
    "UAE": "ARE",
    "QA": "QAT",
    "KW": "KWT",
    "OM": "OMN",
    "OMA": "OMN",
    "BH": "BHR",
    "BAH": "BHR",
}

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

    print(f"🚀 Running ElEsaeiKids pipeline {date_from} → {date_to}")

    # 1. LOAD RAW CSV
    raw_df = fetch_raw_data()
    print("🔍 RAW DF HEAD:")
    print(raw_df.head(10))

    # 2. STORE RAW SNAPSHOT
    store_raw_snapshot(advertiser, raw_df, date_from, date_to, source="el_esaei_csv_sheet")

    # 3. CLEAN
    clean_df = clean_el_esaei(raw_df, advertiser)
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

    # 7. SAVE INTO ElEsaeiKidsTransaction
    count = save_final_rows(advertiser, final_df, date_from, date_to)

    # 8. PUSH TO CAMPAIGN PERFORMANCE
    push_el_esaei_to_performance(date_from, date_to)

    print(f"✅ ElEsaeiKids pipeline inserted {count} rows.")
    return count


# ---------------------------------------------------
# FETCH RAW
# ---------------------------------------------------

def fetch_raw_data() -> pd.DataFrame:
    print("📄 Loading ElEsaeiKids CSV from S3...")
    df = s3_service.read_csv_to_df(S3_CSV_KEY)
    print(f"✅ Loaded {len(df)} rows from S3")
    return df


# ---------------------------------------------------
# CLEAN El_Esaei_Kids FORMAT
# ---------------------------------------------------

def clean_el_esaei(df: pd.DataFrame, advertiser: Advertiser) -> pd.DataFrame:
    """Clean CSV data for ElEsaeiKids."""
    df = df.copy()
    
    df.rename(columns={"date": "created_at", "sales": "sales"}, inplace=True)
    df["created_at"] = pd.to_datetime(df["created_at"], format="%m/%d/%Y", errors="coerce")
    df["sales"] = df["sales"].astype(str).str.replace(",", "").str.replace("%", "").astype(float)
    df["orders"] = df["orders"].astype(int)
    df["coupon"] = df["coupon"].str.upper()
    df["country"] = df["country"].astype(str).str.upper().replace(COUNTRY_MAP)
    
    df["rate_type"] = advertiser.rev_rate_type
    df["commission"] = 0.0
    df["order_id"] = df.apply(lambda row: f"{"ELESAEIKIDS"}_{row['created_at'].strftime('%Y%m%d')}_{row['coupon']}_{row['country']}", axis=1)
    df["user_type"] = "RTU"
    df["order_count"] = df["orders"]
    df["delivery_status"] = "delivered"
    df["partner_id"] = pd.NA
    df["partner_name"] = None
    df["partner_type"] = None
    df["advertiser_id"] = advertiser.id
    df["advertiser_name"] = advertiser.name
    df["currency"] = advertiser.currency
    
    return df


# ---------------------------------------------------
# SAVE FINAL ROWS
# ---------------------------------------------------

def save_final_rows(advertiser: Advertiser, df: pd.DataFrame, date_from: date, date_to: date) -> int:
    if df.empty:
        ElEsaeiKidsTransaction.objects.filter(
            order_date__gte=date_from,
            order_date__lte=date_to
        ).delete()
        return 0

    with transaction.atomic():
        ElEsaeiKidsTransaction.objects.filter(
            order_date__gte=date_from,
            order_date__lte=date_to
        ).delete()

        objs = []
        for r in df.to_dict(orient="records"):
            partner_id = r.get("partner_id")
            partner = None
            if partner_id and not pd.isna(partner_id):
                partner = Partner.objects.filter(id=int(partner_id)).first()
            
            coupon_code = r.get("coupon", "")
            coupon_obj = None
            if coupon_code:
                coupon_obj = Coupon.objects.filter(code=coupon_code, advertiser=advertiser).first()
                
            objs.append(
                ElEsaeiKidsTransaction(
                    order_date=r.get("created_at").date() if pd.notna(r.get("created_at")) else date_from,
                    coupon_code=coupon_code,
                    coupon=coupon_obj,
                    country=r.get("country", ""),
                    orders=nz(r.get("order_count", 1)),
                    sales=nf(r.get("sales")),
                    partner=partner,
                    partner_name=r.get("partner_name", "(No Partner)"),
                    revenue_usd=nf(r.get("our_rev_usd")),
                    payout_usd=nf(r.get("payout_usd")),
                    profit_usd=nf(r.get("profit_usd")),
                )
            )

        ElEsaeiKidsTransaction.objects.bulk_create(objs, batch_size=500)

    return len(df)


# ---------------------------------------------------
# PUSH TO PERFORMANCE
# ---------------------------------------------------

def push_el_esaei_to_performance(date_from, date_to):
    advertiser = Advertiser.objects.filter(name=ADVERTISER_NAME).first()
    qs = ElEsaeiKidsTransaction.objects.filter(
        order_date__gte=date_from,
        order_date__lte=date_to
    )

    if not qs.exists():
        print("⚠️ No ElEsaeiKids transactions found")
        return 0

    groups = {}

    for r in qs:
        key = (
            r.order_date,
            advertiser.name,
            r.partner_name,
            r.coupon_code,
            r.country
        )

        if key not in groups:
            groups[key] = {
                "date": r.order_date,
                "advertiser_name": advertiser.name,
                "partner_name": r.partner_name,
                "coupon": r.coupon_code,
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
        
        # Check if partner is Media Buyer (MB) - they should have zero payout in performance
        partner_obj = Partner.objects.filter(name=r.partner_name).first() if r.partner_name else None
        is_mb = partner_obj and partner_obj.partner_type == "MB"

        # All RDEL transactions are RTU by default
        g["rtu_orders"] += r.orders
        g["rtu_sales"] += float(r.sales) * exchange_rate
        g["rtu_revenue"] += float(r.revenue_usd)
        # MB partners: zero payout in performance (they add costs later)
        g["rtu_payout"] += 0.0 if is_mb else float(r.payout_usd)

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

    print(f"✅ Aggregated {len(objs)} ElEsaeiKids performance rows.")
    return len(objs)
