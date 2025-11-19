# backend/pipelines/drnutrition.py

import pandas as pd
import requests
from io import BytesIO
from datetime import date, datetime
from django.db import transaction
from api.models import Advertiser
from api.models import DrNutritionTransaction
from api.pipelines.helpers import (
    store_raw_snapshot,
    enrich_df,
    resolve_payouts,
    compute_final_metrics,
    nf,
    nz,
)



# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------

ADVERTISER_NAME = "Dr. Nutrition"     # display name
SOURCE_URL = "http://drnutrition.com/growthnify/81df6c4233559d5a5f0cf2b429067307/report"



# ---------------------------------------------------
# MAIN RUN FUNCTION
# ---------------------------------------------------

def run(date_from: date, date_to: date):
    """
    Main function called by the scheduler or management command.
    """

    advertiser = Advertiser.objects.get(name=ADVERTISER_NAME)

    print(f"🚀 Running DrNutrition pipeline {date_from} → {date_to}")

    # 1. FETCH RAW
    raw_df = fetch_raw_data()
    print("🔍 RAW DF HEAD:")
    print(raw_df.head(10))

    # 2. STORE RAW SNAPSHOT
    store_raw_snapshot(advertiser, raw_df, date_from, date_to, source="drnutrition_api")

    # 3. CLEAN → Standardize to required schema
    clean_df = clean_drn(raw_df, advertiser)
    print("🔍 CLEAN DF HEAD:")
    print(clean_df.head(10))

    # 4. ENRICH (coupons → partner mapping)
    enriched_df = enrich_df(clean_df)
    print("🔍 ENRICHED DF HEAD:")
    print(enriched_df.head(10))

    # 5. RESOLVE PAYOUT RULES
    payout_df = resolve_payouts(advertiser, enriched_df)
    print("🔍 PAYOUT DF HEAD:")
    print(payout_df.head(10))

    # 6. FINAL METRICS
    final_df = compute_final_metrics(payout_df, advertiser)
    print("🔍 FINAL DF HEAD:")
    print(final_df.head(10))

    # 7. SAVE RESULTS
    count = save_final_rows(advertiser, final_df, date_from, date_to)
    push_drnut_to_performance(date_from, date_to)

    print(f"✅ DrNutrition pipeline inserted {count} rows.")
    return count



# ---------------------------------------------------
# STEP 1 — FETCH RAW DATA
# ---------------------------------------------------

def fetch_raw_data() -> pd.DataFrame:
    """
    Download the raw file from DrNutrition & load into a DataFrame.
    Works for Excel or CSV.
    """

    print("📡 Fetching DrNutrition data...")

    response = requests.get(SOURCE_URL)

    if response.status_code != 200:
        raise Exception(f"❌ Failed to download file: {response.status_code}")

    file_like = BytesIO(response.content)

    # Try Excel first
    try:
        df = pd.read_excel(file_like)
    except:
        file_like.seek(0)
        df = pd.read_csv(file_like)

    print(f"✅ Loaded {len(df)} raw rows.")
    return df



# ---------------------------------------------------
# STEP 2 — CLEANING / NORMALIZATION
# ---------------------------------------------------

import json

def clean_drn(df: pd.DataFrame, advertiser: Advertiser) -> pd.DataFrame:
    print("🧹 Cleaning DrNutrition data...")

    # ----- Rename -----
    df = df.rename(columns={
        "Order ID": "order_id",
        "Created Date": "created_at",
        "Selling Price": "sales",
        "commission": "commission_value",
        "Campaign": "campaign",
        "Code": "coupon",
        "Logs": "logs",
        "Delivery Status": "delivery_status",
        "Type": "user_type",
    })

    # ✅ Enrichment-safe placeholders (leave advertiser_id empty here)
    for col, default in {
        "partner_id": pd.NA,
        "partner_name": None,
        "partner_type": None,
        "advertiser_id": pd.NA,
        "advertiser_name": None,
    }.items():
        if col not in df.columns:
            df[col] = default
    df["partner_id"] = pd.to_numeric(df["partner_id"], errors="coerce").astype("Int64")

    # ----- DATE -----
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    # ----- DELIVERY STATUS -----
    df["delivery_status"] = df["delivery_status"].fillna("").astype(str)
    df = df[df["delivery_status"].str.lower() != "canceled"]

    # ----- COUNTRY NORMALIZATION -----
    country_map = {
        "UNITED ARAB EMIRATES": "ARE","UAE": "ARE","SAUDI ARABIA": "SAU","KSA": "SAU",
        "KUWAIT": "KWT","PAKISTAN": "PAK","QATAR": "QAT","QTR": "QAT",
        "OMAN": "OMN","JORDAN": "JOR","BAHRAIN": "BHR",
    }
    df["country"] = df["country"].astype(str).str.strip().str.upper().replace(country_map)

    # ----- FTU / RTU -----
    df["user_type"] = df["user_type"].fillna("FTU").replace({"Sale": "RTU"})
    df["ftu_orders"] = (df["user_type"] == "FTU").astype(int)
    df["rtu_orders"] = (df["user_type"] == "RTU").astype(int)
    df["orders"] = 1

    # ----- MONEY -----
    df["sales"] = pd.to_numeric(df["sales"], errors="coerce").fillna(0)
    df["commission"] = pd.to_numeric(df["commission_value"], errors="coerce").fillna(0)

    # ----- FIXED -----
    df["currency"] = advertiser.currency
    df["rate_type"] = advertiser.rev_rate_type
    return df



def save_final_rows(advertiser: Advertiser, df: pd.DataFrame, date_from: date, date_to: date) -> int:
    """Store output rows into DrNutritionTransaction."""

    if df.empty:
        DrNutritionTransaction.objects.filter(
            created_date__date__gte=date_from,
            created_date__date__lte=date_to
        ).delete()
        return 0

    with transaction.atomic():
        DrNutritionTransaction.objects.filter(
            created_date__date__gte=date_from,
            created_date__date__lte=date_to
        ).delete()

        objs = []
        for r in df.to_dict(orient="records"):
            objs.append(
                DrNutritionTransaction(
                    order_id=r["order_id"],
                    created_date=r.get("created_at"),      # ✅ FIXED
                    delivery_status=r.get("delivery_status", "") or "",   # ✅ FIXED
                    country=r.get("country"),
                    coupon=r.get("coupon"),
                    user_type=r.get("user_type"),
                    partner_name=r.get("partner_name"),
                    partner_type=r.get("partner_type"),
                    advertiser_name=r.get("advertiser_name") or "",
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

        DrNutritionTransaction.objects.bulk_create(objs, batch_size=2000)

    return len(df)


from django.db import transaction
from django.db.models import Sum
from api.models import (
    DrNutritionTransaction,
    CampaignPerformance,
    Advertiser,
    Partner,
    Coupon,
)

def push_drnut_to_performance(date_from, date_to):
    advertiser = Advertiser.objects.get(name=ADVERTISER_NAME)
    """
    Aggregate DrNutritionTransaction rows into CampaignPerformance.
    Row-based logic for FTU / RTU orders, sales, revenue, payout.
    """

    qs = DrNutritionTransaction.objects.filter(
        created_date__date__gte=date_from,
        created_date__date__lte=date_to
    )

    if not qs.exists():
        print("⚠️ No DrNutritionTransaction rows found for this range.")
        return 0

    # ---------------------------------------------------------------------
    # GROUP BY granular keys
    # date + advertiser + partner + coupon + geo
    # ---------------------------------------------------------------------
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
                # core IDs/names
                "date": r.created_date.date(),
                "advertiser_name": r.advertiser_name,
                "partner_name": r.partner_name,
                "coupon": r.coupon,
                "geo": r.country,

                # metrics
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

        # -------------------------
        # Row-based accumulation
        # -------------------------
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

    # ---------------------------------------------------------------------
    # Save results to CampaignPerformance
    # ---------------------------------------------------------------------
    with transaction.atomic():
        # delete old performance rows for this date range
        CampaignPerformance.objects.filter(
            advertiser=advertiser,
            date__gte=date_from,
            date__lte=date_to
        ).delete()

        objs = []
        for key, g in groups.items():

            advertiser = Advertiser.objects.filter(
                name=g["advertiser_name"]
            ).first()

            partner = Partner.objects.filter(
                name=g["partner_name"]
            ).first() if g["partner_name"] else None

            coupon_obj = Coupon.objects.filter(
                code=g["coupon"]
            ).first()

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