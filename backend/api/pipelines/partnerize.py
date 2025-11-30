# backend/api/pipelines/partnerize.py

import pandas as pd
import base64
import requests
import json
from datetime import date, datetime, timedelta
from django.db import transaction
from api.models import Advertiser, PartnerizeConversion
from api.pipelines.helpers import (
    store_raw_snapshot,
    enrich_df,
    resolve_payouts,
    compute_final_metrics,
    nf,
    nz,
)
import uuid

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------

ADVERTISER_NAME = "Partnerize"  # Make sure this exists in your Advertiser table
APP_KEY = "XHBaFY3jOY"
API_KEY = "9QQPhhOW"
PUBLISHER_ID = "1011l405470"
BASE_URL_V1 = "https://api.partnerize.com/reporting/report_publisher/publisher"


# ---------------------------------------------------
# MAIN RUN FUNCTION
# ---------------------------------------------------

def run(date_from: date, date_to: date):
    """
    Main function called by the scheduler or management command.
    """
    
    advertiser = Advertiser.objects.get(name=ADVERTISER_NAME)
    
    print(f"🚀 Running Partnerize pipeline {date_from} → {date_to}")
    
    # 1. FETCH RAW
    raw_df = fetch_raw_data(date_from, date_to)
    print("🔍 RAW DF HEAD:")
    print(raw_df.head(10))
    
    # 2. STORE RAW SNAPSHOT
    store_raw_snapshot(advertiser, raw_df, date_from, date_to, source="partnerize_api")
    
    # 3. CLEAN → Standardize to required schema
    clean_df = clean_partnerize(raw_df, advertiser)
    print("🔍 CLEAN DF HEAD:")
    print(clean_df.head(10))
    
    # 4. ENRICH (coupons → partner mapping)
    enriched_df = enrich_df(clean_df)
    print("🔍 ENRICHED DF HEAD:")
    print(enriched_df.head(10))
    
    # 5. RESOLVE PAYOUT RULES
    payout_df = resolve_payouts_with_history(advertiser, enriched_df, timestamp_col="conversion_time")
    print("🔍 PAYOUT DF HEAD:")
    print(payout_df.head(10))
    
    # 6. FINAL METRICS
    final_df = compute_final_metrics(payout_df, advertiser)
    print("🔍 FINAL DF HEAD:")
    print(final_df.head(10))
    
    # 7. SAVE RESULTS
    count = save_final_rows(advertiser, final_df, date_from, date_to)
    push_partnerize_to_performance(date_from, date_to)
    
    print(f"✅ Partnerize pipeline inserted {count} rows.")
    return count


# ---------------------------------------------------
# STEP 1 — FETCH RAW DATA
# ---------------------------------------------------

def make_auth_header(app_key, api_key):
    token = f"{app_key}:{api_key}"
    b64 = base64.b64encode(token.encode("utf-8")).decode("utf-8")
    return {
        "Authorization": f"Basic {b64}",
        "Accept": "application/json"
    }

def fetch_raw_data(date_from: date, date_to: date) -> pd.DataFrame:
    """
    Fetch conversions from Partnerize API for date range.
    """
    print("📡 Fetching Partnerize data...")
    
    # Convert dates to ISO format with time
    start_str = datetime.combine(date_from, datetime.min.time()).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = datetime.combine(date_to, datetime.max.time()).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    url = f"{BASE_URL_V1}/{PUBLISHER_ID}/conversion.json"
    headers = make_auth_header(APP_KEY, API_KEY)
    params = {
        "start_date": start_str,
        "end_date": end_str
    }
    
    resp = requests.get(url, headers=headers, params=params)
    print(f"API Status: {resp.status_code}")
    print(f"API Response: {resp.text[:500]}")
    resp.raise_for_status()
    
    report = resp.json()
    df = extract_conversions(report)
    
    print(f"✅ Loaded {len(df)} raw rows.")
    return df

def extract_conversions(report):
    conversions = report.get("conversions", [])
    rows = []

    for conv in conversions:
        data = conv.get("conversion_data", {})
        if not data:
            continue

        voucher = None
        items = data.get("conversion_items", [])
        if items:
            first_item = items[0]
            vouchers = first_item.get("voucher_codes", [])
            if vouchers:
                voucher = vouchers[0].get("voucher_code")

        row = {
            "conversion_id": data.get("conversion_id"),
            "campaign_title": data.get("campaign_title"),
            "conversion_time": data.get("conversion_time"),
            "country": data.get("country"),
            "total_order_value": data.get("conversion_value", {}).get("value"),
            "total_commission": data.get("conversion_value", {}).get("publisher_commission"),
            "conversion_status": data.get("conversion_value", {}).get("conversion_status"),
            "voucher": voucher,
            "first_time_user": data.get("meta_data", {}).get("first_time_transaction")
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    return df


# ---------------------------------------------------
# STEP 2 — CLEANING / NORMALIZATION
# ---------------------------------------------------

def clean_partnerize(df: pd.DataFrame, advertiser: Advertiser) -> pd.DataFrame:
    print("🧹 Cleaning Partnerize data...")
    
    # Rename to standard schema
    df = df.rename(columns={
        "conversion_id": "order_id",
        "conversion_time": "created_at",
        "total_order_value": "sales",
        "total_commission": "commission",
        "voucher": "coupon",
        "campaign_title": "campaign",
    })
    
    # Date parsing
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    
    # User type mapping
    df["user_type"] = df["first_time_user"].apply(
        lambda x: "FTU" if x == True or str(x).lower() == "true" else "RTU"
    )
    
    # Enrich placeholders
    for col, default in {
        "partner_id": pd.NA,
        "partner_name": None,
        "partner_type": None,
        "advertiser_id": pd.NA,
        "advertiser_name": advertiser.name,
    }.items():
        if col not in df.columns:
            df[col] = default
    
    df["partner_id"] = pd.to_numeric(df["partner_id"], errors="coerce").astype("Int64")
    
    # Numeric fields
    df["sales"] = pd.to_numeric(df["sales"], errors="coerce").fillna(0)
    df["commission"] = pd.to_numeric(df["commission"], errors="coerce").fillna(0)
    
    # Add tracking fields
    df["orders"] = 1
    df["ftu_orders"] = (df["user_type"] == "FTU").astype(int)
    df["rtu_orders"] = (df["user_type"] == "RTU").astype(int)
    
    # Filter valid rows
    df = df[df["created_at"].notna()]
    df = df[df["sales"] > 0]
    
    print(f"✅ Cleaned to {len(df)} valid rows.")
    return df


# ---------------------------------------------------
# STEP 7 — SAVE TO DATABASE
# ---------------------------------------------------

def save_final_rows(advertiser: Advertiser, df: pd.DataFrame, date_from: date, date_to: date) -> int:
    """
    Save final processed rows to PartnerizeConversion table.
    """
    with transaction.atomic():
        # Delete old records in this date range
        PartnerizeConversion.objects.filter(
            conversion_time__date__gte=date_from,
            conversion_time__date__lte=date_to
        ).delete()

        objs = []
        for _, r in df.iterrows():
            objs.append(
                PartnerizeConversion(
                    uuid=uuid.uuid4(),
                    conversion_id=str(r.get("order_id", "")),
                    campaign_title=r.get("campaign", ""),
                    conversion_time=r.get("created_at"),
                    country=r.get("country", ""),
                    total_order_value=nf(r.get("sales")),
                    total_commission=nf(r.get("commission")),
                    conversion_status=r.get("conversion_status", ""),
                    voucher=r.get("coupon", ""),
                    first_time_user=r.get("user_type") == "FTU",
                    partner_name=r.get("partner_name"),
                    partner_type=r.get("partner_type"),
                    advertiser_name=advertiser.name,
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

        PartnerizeConversion.objects.bulk_create(objs, batch_size=2000)

    return len(df)


# ---------------------------------------------------
# STEP 8 — PUSH TO CAMPAIGNPERFORMANCE
# ---------------------------------------------------

from api.models import CampaignPerformance, Partner, Coupon

def push_partnerize_to_performance(date_from: date, date_to: date):
    """
    Aggregate PartnerizeConversion rows into CampaignPerformance.
    """
    advertiser = Advertiser.objects.get(name=ADVERTISER_NAME)
    
    qs = PartnerizeConversion.objects.filter(
        conversion_time__date__gte=date_from,
        conversion_time__date__lte=date_to
    )
    
    if not qs.exists():
        print("⚠️ No PartnerizeConversion rows found for this range.")
        return 0
    
    # Group by date + partner + coupon + geo
    groups = {}
    for r in qs:
        key = (
            r.conversion_time.date(),
            r.advertiser_name,
            r.partner_name,
            r.voucher,
            r.country
        )
        
        if key not in groups:
            groups[key] = {
                "date": r.conversion_time.date(),
                "advertiser_name": r.advertiser_name,
                "partner_name": r.partner_name,
                "coupon": r.voucher,
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
        exchange_rate = float(advertiser.exchange_rate or 1.0)
        
        if r.first_time_user:
            g["ftu_orders"] += r.orders
            g["ftu_sales"] += float(r.sales) * exchange_rate
            g["ftu_revenue"] += float(r.our_rev) * exchange_rate
            g["ftu_payout"] += float(r.payout) * exchange_rate
        else:
            g["rtu_orders"] += r.orders
            g["rtu_sales"] += float(r.sales) * exchange_rate
            g["rtu_revenue"] += float(r.our_rev) * exchange_rate
            g["rtu_payout"] += float(r.payout) * exchange_rate
    
    # Save to CampaignPerformance
    with transaction.atomic():
        CampaignPerformance.objects.filter(
            advertiser=advertiser,
            date__gte=date_from,
            date__lte=date_to
        ).delete()
        
        objs = []
        for key, g in groups.items():
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
                    ftu_our_rev=g["ftu_revenue"],
                    rtu_our_rev=g["rtu_revenue"],
                    total_our_rev=g["ftu_revenue"] + g["rtu_revenue"],
                    ftu_payout=g["ftu_payout"],
                    rtu_payout=g["rtu_payout"],
                    total_payout=g["ftu_payout"] + g["rtu_payout"],
                )
            )
        
        CampaignPerformance.objects.bulk_create(objs, batch_size=2000)
    
    print(f"✅ Aggregated {len(objs)} performance rows.")
    return len(objs)


# ---------------------------------------------------
# OLD STANDALONE FUNCTION (KEPT FOR BACKWARD COMPATIBILITY)
# ---------------------------------------------------

def load_partnerize_data_to_db():
    end = datetime.utcnow()
    start = end - timedelta(days=90)

    start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end.strftime("%Y-%m-%dT%H:%M:%SZ")

    report_v1 = get_v1_conversion_report(start_str, end_str)
    if not report_v1:
        print("⚠️ No data in report_v1.")
        return

    df = extract_conversions(report_v1)
    if df.empty:
        print("⚠️ No conversions extracted.")
        return

    created_count = 0
    for _, row in df.iterrows():
        try:
            obj = PartnerizeConversion(
                uuid=uuid.uuid4(),
                conversion_id=row["conversion_id"],
                campaign_title=row["campaign_title"],
                conversion_time=row["conversion_time"],
                country=row["country"],
                total_order_value=row["total_order_value"],
                total_commission=row["total_commission"],
                conversion_status=row["conversion_status"],
                voucher=row["voucher"],
                first_time_user=row["first_time_user"]
            )
            obj.save()
            created_count += 1
        except Exception as e:
            print(f"❌ Error saving row: {e}")

    print(f"✅ Loaded {created_count} Partnerize conversions.")


if __name__ == "__main__":
    load_partnerize_data_to_db()