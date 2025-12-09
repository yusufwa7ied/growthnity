# backend/api/pipelines/namshi_only.py
"""
Pipeline for processing NAMSHI orders ONLY.
Uses percentage-based payouts from PartnerPayout table.
"""

import pandas as pd
from datetime import date
from django.db import transaction
from django.conf import settings

from api.models import (
    Advertiser,
    NamshiTransaction,
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

ADVERTISER_NAME = "Namshi"
S3_CSV_KEY = settings.S3_PIPELINE_FILES["namshi"]  # Namshi tab from Google Sheets

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


def run(date_from: date, date_to: date):
    """Main function to process Namshi orders with percentage-based payouts."""
    print(f"🚀 Running Namshi pipeline {date_from} → {date_to}")
    
    advertiser = Advertiser.objects.filter(name=ADVERTISER_NAME).first()
    if not advertiser:
        print(f"❌ Advertiser '{ADVERTISER_NAME}' not found in database")
        return 0
    
    # 1. Fetch raw data from S3
    raw_df = fetch_raw_data()
    
    # 2. Filter to Namshi orders only
    raw_df = raw_df[raw_df["Advertiser"].astype(str).str.strip() == ADVERTISER_NAME]
    print(f"📊 Filtered to {len(raw_df)} Namshi rows")
    
    if raw_df.empty:
        print("⚠️ No Namshi orders found in date range")
        return 0
    
    # 3. Store raw snapshot
    store_raw_snapshot(advertiser, raw_df, date_from, date_to, source="namshi_csv")
    
    # 4. Clean and normalize data
    clean_df = clean_namshi(raw_df)
    
    # 5. Enrich with partner/coupon mapping
    enriched_df = enrich_df(clean_df, advertiser=advertiser)
    
    # 6. Resolve payouts using percentage rates from PartnerPayout table
    payout_df = resolve_payouts_with_history(advertiser, enriched_df)
    
    # 7. Compute final metrics (revenue, profit, etc.)
    final_df = compute_final_metrics(payout_df, advertiser)
    
    # 8. Save to database
    count = save_final_rows(advertiser, final_df, date_from, date_to)
    
    # 9. Push to CampaignPerformance aggregation table
    push_to_performance(advertiser, date_from, date_to)
    
    print(f"✅ Namshi pipeline inserted {count} rows.")
    return count


def fetch_raw_data() -> pd.DataFrame:
    """Fetch CSV from S3"""
    print("📄 Loading Namshi CSV from S3...")
    df = s3_service.read_csv_to_df(S3_CSV_KEY)
    print(f"✅ Loaded {len(df)} total rows")
    return df


def clean_namshi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and normalize Namshi data.
    
    Input columns:
      Order Date | Advertiser | Country | Coupon Code | Total orders | NON-PAYABLE Orders
      | Total Order Value | FTU Orders | FTU Order Values | RTU Orders | RTU Order Value | Platform
    
    Output: Split FTU/RTU into separate rows with user_type.
    """
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
            "orders": orders,
            "ftu_orders": orders if user_type == "FTU" else 0,
            "rtu_orders": orders if user_type == "RTU" else 0,
            "sales": sales,
            "commission": 0.0,
            "currency": None,
            "rate_type": None,
        })

    for _, r in df.iterrows():
        emit_row(r, "FTU", "ftu_orders_src", "ftu_value")
        emit_row(r, "RTU", "rtu_orders_src", "rtu_value")

    return pd.DataFrame(rows)


def save_final_rows(advertiser: Advertiser, df: pd.DataFrame, date_from: date, date_to: date) -> int:
    """Save processed rows to NamshiTransaction table"""
    if df.empty:
        NamshiTransaction.objects.filter(
            advertiser_name=advertiser.name,
            created_date__gte=date_from,
            created_date__lte=date_to,
        ).delete()
        return 0

    with transaction.atomic():
        NamshiTransaction.objects.filter(
            advertiser_name=advertiser.name,
            created_date__gte=date_from,
            created_date__lte=date_to,
        ).delete()

        objs = []
        for r in df.to_dict(orient="records"):
            partner_id = r.get("partner_id")
            partner = None
            if partner_id and not pd.isna(partner_id):
                partner = Partner.objects.filter(id=int(partner_id)).first()

            objs.append(
                NamshiTransaction(
                    order_id=0,
                    created_date=r.get("created_at"),
                    delivery_status="delivered",
                    country=r.get("country"),
                    coupon=r.get("coupon"),
                    user_type=r.get("user_type"),
                    partner=partner,
                    partner_name=r.get("partner_name"),
                    partner_type=r.get("partner_type"),
                    advertiser_name=advertiser.name,
                    currency=advertiser.currency or "AED",
                    rate_type=advertiser.rev_rate_type or "percent",
                    sales=nf(r.get("sales")),
                    commission=nf(r.get("commission", 0)),
                    our_rev=nf(r.get("our_rev", 0)),
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

        NamshiTransaction.objects.bulk_create(objs, batch_size=2000)

    return len(df)


def push_to_performance(advertiser: Advertiser, date_from: date, date_to: date):
    """Aggregate to CampaignPerformance table"""
    qs = NamshiTransaction.objects.filter(
        advertiser_name=advertiser.name,
        created_date__date__gte=date_from,
        created_date__date__lte=date_to
    )
    
    if not qs.exists():
        print("⚠️ No Namshi rows to aggregate.")
        return 0

    exchange_rate = float(advertiser.exchange_rate or 1.0)
    groups = {}
    
    for r in qs:
        key = (r.created_date.date(), r.partner_name, r.coupon, r.country)
        
        if key not in groups:
            groups[key] = {
                "date": r.created_date.date(),
                "partner_name": r.partner_name,
                "coupon": r.coupon,
                "geo": r.country,
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

    with transaction.atomic():
        CampaignPerformance.objects.filter(
            advertiser=advertiser,
            date__gte=date_from,
            date__lte=date_to
        ).delete()

        objs = []
        for _, g in groups.items():
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

    print(f"✅ Aggregated {len(objs)} Namshi performance rows.")
    return len(objs)
