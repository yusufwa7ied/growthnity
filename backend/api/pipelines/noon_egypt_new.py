# backend/api/pipelines/noon_egypt.py
"""
Pipeline for processing Noon Egypt orders.
Uses bracket-based payouts from the Google Sheet.
Sheet structure: ID, Date, Tag (ftu/rtu), Coupon Code, #order (hash), Bracket, order_value_gmv_usd
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
    nf,
    nz,
)
from api.services.s3_service import s3_service

ADVERTISER_NAME = "Noon"
S3_EGYPT_KEY = settings.S3_PIPELINE_FILES["noon_egypt"]


def run(date_from: date, date_to: date):
    """Main pipeline execution."""
    print(f"🚀 Running Noon Egypt pipeline {date_from} → {date_to}")
    
    advertiser = Advertiser.objects.filter(name=ADVERTISER_NAME).first()
    if not advertiser:
        print(f"❌ Advertiser '{ADVERTISER_NAME}' not found")
        return 0
    
    # 1. Fetch raw data
    raw_df = fetch_raw_data()
    
    if raw_df.empty:
        print("⚠️ No Noon Egypt data found")
        return 0
    
    print(f"📊 Retrieved {len(raw_df)} Noon Egypt rows")
    
    # 2. Store raw snapshot
    store_raw_snapshot(advertiser, raw_df, date_from, date_to, source="noon_egypt_csv")
    
    # 3. Clean and transform
    clean_df = clean_noon_egypt(raw_df)
    print(f"✅ Cleaned {len(clean_df)} rows")
    
    # 4. Enrich with coupon/partner mapping
    enriched_df = enrich_with_coupons(clean_df)
    print(f"✅ Enriched {len(enriched_df)} rows")
    
    # 5. Save to NoonEgyptTransaction
    count = save_final_rows(advertiser, enriched_df, date_from, date_to)
    
    # 6. Push to CampaignPerformance
    push_to_performance(advertiser, date_from, date_to)
    
    print(f"✅ Noon Egypt pipeline inserted {count} rows")
    return count


def fetch_raw_data() -> pd.DataFrame:
    """Fetch Noon Egypt data from S3."""
    print("📄 Loading Noon Egypt CSV from S3...")
    df = s3_service.read_csv_to_df(S3_EGYPT_KEY)
    print(f"✅ Loaded {len(df)} rows from S3")
    return df


def clean_noon_egypt(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform Noon Egypt data.
    Expected columns: ID, Date, Tag, Coupon Code, #order, Bracket, order_value_gmv_usd
    """
    # Rename columns to match our model
    df = df.rename(columns={
        "ID": "record_id",
        "Date": "order_date",
        "Tag": "user_type",
        "Coupon Code": "coupon_code",
        "#order": "order_hash",
        "Bracket": "bracket",
        "order_value_gmv_usd": "order_value_usd",
    })
    
    # Clean data types
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["record_id"] = df["record_id"].astype(str)
    df["order_hash"] = df["order_hash"].astype(str)
    df["coupon_code"] = df["coupon_code"].astype(str).str.strip().str.upper()
    df["user_type"] = df["user_type"].astype(str).str.lower().str.strip()
    df["bracket"] = df["bracket"].astype(str).str.strip()
    df["order_value_usd"] = pd.to_numeric(df["order_value_usd"], errors="coerce").fillna(0.0)
    
    # Extract bracket payout from bracket string
    # e.g., "Bracket 1_$0.27" → 0.27
    def extract_bracket_payout(bracket_str):
        try:
            if "_$" in bracket_str:
                return float(bracket_str.split("_$")[1])
            elif "Below Bracket Range" in bracket_str:
                return 0.0
            return 0.0
        except:
            return 0.0
    
    df["bracket_payout_usd"] = df["bracket"].apply(extract_bracket_payout)
    
    # Filter out rows with invalid dates
    df = df[df["order_date"].notna()].copy()
    
    # Calculate revenue (we get 15% of GMV)
    df["revenue_usd"] = df["order_value_usd"] * 0.15
    
    # Payout is the bracket amount
    df["payout_usd"] = df["bracket_payout_usd"]
    
    # Profit = revenue - payout
    df["profit_usd"] = df["revenue_usd"] - df["payout_usd"]
    
    return df


def enrich_with_coupons(df: pd.DataFrame) -> pd.DataFrame:
    """Enrich with Coupon and Partner foreign keys."""
    coupon_cache = {}
    partner_cache = {}
    
    def get_coupon(code):
        if code not in coupon_cache:
            coupon_cache[code] = Coupon.objects.filter(code=code).first()
        return coupon_cache[code]
    
    def get_partner(coupon_obj):
        if not coupon_obj:
            return None, None
        partner_id = coupon_obj.partner_id
        if partner_id not in partner_cache:
            partner_cache[partner_id] = coupon_obj.partner
        return partner_cache[partner_id], partner_cache[partner_id].name if partner_cache[partner_id] else None
    
    enriched_rows = []
    for _, row in df.iterrows():
        coupon_obj = get_coupon(row["coupon_code"])
        partner_obj, partner_name = get_partner(coupon_obj)
        
        enriched_rows.append({
            **row.to_dict(),
            "coupon": coupon_obj,
            "partner": partner_obj,
            "partner_name": partner_name or "",
        })
    
    return pd.DataFrame(enriched_rows)


def save_final_rows(advertiser: Advertiser, df: pd.DataFrame, date_from: date, date_to: date) -> int:
    """Save to NoonEgyptTransaction table."""
    if df.empty:
        NoonEgyptTransaction.objects.filter(
            order_date__gte=date_from,
            order_date__lte=date_to
        ).delete()
        return 0
    
    with transaction.atomic():
        # Delete existing records in date range
        NoonEgyptTransaction.objects.filter(
            order_date__gte=date_from,
            order_date__lte=date_to
        ).delete()
        
        objs = []
        for _, r in df.iterrows():
            order_date_val = r.get("order_date")
            if hasattr(order_date_val, 'date'):
                order_date_val = order_date_val.date()
            
            objs.append(
                NoonEgyptTransaction(
                    record_id=str(r.get("record_id", "")),
                    order_hash=str(r.get("order_hash", "")),
                    order_date=order_date_val,
                    coupon=r.get("coupon"),
                    coupon_code=str(r.get("coupon_code", "")),
                    partner=r.get("partner"),
                    partner_name=str(r.get("partner_name", "")),
                    user_type=str(r.get("user_type", "")).lower(),
                    bracket=str(r.get("bracket", "")),
                    bracket_payout_usd=Decimal(str(r.get("bracket_payout_usd", 0))),
                    order_value_usd=Decimal(str(r.get("order_value_usd", 0))),
                    revenue_usd=Decimal(str(r.get("revenue_usd", 0))),
                    payout_usd=Decimal(str(r.get("payout_usd", 0))),
                    profit_usd=Decimal(str(r.get("profit_usd", 0))),
                )
            )
        
        NoonEgyptTransaction.objects.bulk_create(objs, batch_size=2000)
    
    return len(objs)


def push_to_performance(advertiser: Advertiser, date_from: date, date_to: date):
    """Aggregate to CampaignPerformance."""
    qs = NoonEgyptTransaction.objects.filter(
        order_date__gte=date_from,
        order_date__lte=date_to
    )
    
    if not qs.exists():
        print("⚠️ No Noon Egypt rows to aggregate")
        return 0
    
    groups = {}
    
    for r in qs:
        key = (r.order_date, r.partner_name, r.coupon_code)
        
        if key not in groups:
            groups[key] = {
                "date": r.order_date,
                "partner_name": r.partner_name,
                "coupon": r.coupon_code,
                "ftu_orders": 0,
                "rtu_orders": 0,
                "ftu_sales": 0.0,
                "rtu_sales": 0.0,
                "ftu_revenue": 0.0,
                "rtu_revenue": 0.0,
                "ftu_payout": 0.0,
                "rtu_payout": 0.0,
            }
        
        g = groups[key]
        
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
    
    with transaction.atomic():
        CampaignPerformance.objects.filter(
            advertiser=advertiser,
            date__gte=date_from,
            date__lte=date_to,
            geo="EGY"
        ).delete()
        
        objs = []
        for _, g in groups.items():
            partner = Partner.objects.filter(name=g["partner_name"]).first() if g["partner_name"] else None
            coupon_obj = Coupon.objects.filter(code=g["coupon"]).first() if g["coupon"] else None
            
            # Skip records with blank coupon
            if not coupon_obj:
                continue
            
            objs.append(
                CampaignPerformance(
                    date=g["date"],
                    advertiser=advertiser,
                    partner=partner,
                    coupon=coupon_obj,
                    geo="EGY",
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
    
    print(f"✅ Aggregated {len(objs)} Noon Egypt performance rows")
    return len(objs)
