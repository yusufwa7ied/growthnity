# backend/api/pipelines/reef.py

import pandas as pd
from datetime import date, datetime
from django.db import transaction
from django.conf import settings

from api.models import (
    Advertiser,
    ReefTransaction,
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

# Country mapping - Arabic to 3-letter ISO codes
COUNTRY_MAP = {
    # Arabic names
    "الامارات": "ARE",
    "قطر": "QAT",
    "البحرين": "BHR",
    "المملكة العربية السعودية": "SAU",
    "الكويت": "KWT",
    "عمان": "OMN",
    # English names (fallback)
    "KSA": "SAU",
    "UAE": "ARE",
    "QA": "QAT",
    "KW": "KWT",
    "OM": "OMN",
    "OMA": "OMN",
    "BH": "BHR",
    "BAH": "BHR",
}

# User type mapping - Arabic to English
USER_TYPE_MAP = {
    "جديد": "FTU",  # New customer
    "مكرر": "RTU",  # Repeat customer
}

# Delivery status mapping - Arabic to English
DELIVERY_STATUS_MAP = {
    "تم التوصيل": "delivered",
    "جاري التوصيل": "in_transit",
    "تم التنفيذ": "fulfilled",
}

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------

ADVERTISER_NAME = "Reef"
S3_CSV_KEY = "pipeline-data/reef.csv"


# ---------------------------------------------------
# MAIN RUN FUNCTION
# ---------------------------------------------------

def run(date_from: date, date_to: date):
    advertiser = Advertiser.objects.get(name=ADVERTISER_NAME)

    print(f"🚀 Running Reef pipeline {date_from} → {date_to}")

    # 1. LOAD RAW CSV
    raw_df = fetch_raw_data()
    print("🔍 RAW DF HEAD:")
    print(raw_df.head(10))

    # 2. STORE RAW SNAPSHOT
    store_raw_snapshot(advertiser, raw_df, date_from, date_to, source="reef_csv_sheet")

    # 3. CLEAN
    clean_df = clean_reef(raw_df, advertiser)
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

    # 7. SAVE INTO ReefTransaction
    count = save_final_rows(advertiser, final_df, date_from, date_to)

    # 8. PUSH TO CAMPAIGN PERFORMANCE
    push_reef_to_performance(date_from, date_to)

    print(f"✅ Reef pipeline inserted {count} rows.")
    return count


# ---------------------------------------------------
# FETCH RAW
# ---------------------------------------------------

def fetch_raw_data() -> pd.DataFrame:
    print("📄 Loading Reef CSV from S3...")
    df = s3_service.read_csv_to_df(S3_CSV_KEY)
    print(f"✅ Loaded {len(df)} rows from S3")
    return df


# ---------------------------------------------------
# CLEAN Reef FORMAT
# ---------------------------------------------------

def clean_reef(df: pd.DataFrame, advertiser: Advertiser) -> pd.DataFrame:
    """
    Clean Excel data for Reef (Arabic column names).
    
    Expected columns:
    - Date - Year, Date - Quarter, Date - Month, Date - Day
    - كود الكوبون (Coupon code)
    - صافى المبيعات (Net sales)
    - تصنيف العميل (Customer type: جديد=FTU, مكرر=RTU)
    - الدول (Country)
    - رقم الطلب (Order number)
    - حالة الطلب (Order status)
    """
    df = df.copy()
    
    # Remove summary/total rows (rows where Date - Year contains "Total" or filter text)
    df = df[df["Date - Year"].astype(str).str.isdigit()].copy()
    
    # Build date from components
    df["year"] = df["Date - Year"].astype(int)
    df["month"] = pd.to_datetime(df["Date - Month"], format="%B", errors="coerce").dt.month
    df["day"] = df["Date - Day"].astype(int)
    df["created_at"] = pd.to_datetime(df[["year", "month", "day"]], errors="coerce")
    
    # Map Arabic column names to English
    df.rename(columns={
        "كود الكوبون": "coupon",
        "صافى المبيعات": "sales",
        "تصنيف العميل": "user_type_arabic",
        "الدول": "country_arabic",
        "رقم الطلب": "order_number",
        "حالة الطلب": "delivery_status_arabic",
    }, inplace=True)
    
    # Clean and transform data
    # Remove commas from sales values before converting to float
    df["sales"] = df["sales"].astype(str).str.replace(',', '', regex=False).astype(float)
    df["coupon"] = df["coupon"].astype(str).str.strip().str.upper()
    df["country"] = df["country_arabic"].astype(str).str.strip().replace(COUNTRY_MAP)
    df["user_type"] = df["user_type_arabic"].astype(str).str.strip().replace(USER_TYPE_MAP)
    df["delivery_status"] = df["delivery_status_arabic"].astype(str).str.strip().replace(DELIVERY_STATUS_MAP)
    df["order_number"] = df["order_number"].astype(int)
    
    # Each row is 1 order
    df["order_count"] = 1
    df["orders"] = 1
    
    # Build order_id (unique identifier)
    df["order_id"] = df["order_number"].astype(str)
    
    # Standard fields for pipeline
    df["rate_type"] = advertiser.rev_rate_type
    df["commission"] = 0.0
    df["partner_id"] = pd.NA
    df["partner_name"] = None
    df["partner_type"] = None
    df["advertiser_id"] = advertiser.id
    df["advertiser_name"] = advertiser.name
    df["currency"] = advertiser.currency
    
    # Drop temporary columns
    df = df.drop(columns=["Date - Year", "Date - Quarter", "Date - Month", "Date - Day", 
                          "year", "month", "day", "user_type_arabic", "country_arabic", 
                          "delivery_status_arabic", "order_number"], errors="ignore")
    
    return df


# ---------------------------------------------------
# SAVE FINAL ROWS
# ---------------------------------------------------

def save_final_rows(advertiser: Advertiser, df: pd.DataFrame, date_from: date, date_to: date) -> int:
    if df.empty:
        ReefTransaction.objects.filter(
            order_date__gte=date_from,
            order_date__lte=date_to
        ).delete()
        return 0

    with transaction.atomic():
        ReefTransaction.objects.filter(
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
                ReefTransaction(
                    order_date=r.get("created_at").date() if pd.notna(r.get("created_at")) else date_from,
                    coupon_code=coupon_code,
                    coupon=coupon_obj,
                    country=r.get("country", ""),
                    user_type=r.get("user_type", "RTU"),  # NEW: FTU or RTU from cleaned data
                    orders=nz(r.get("order_count", 1)),
                    sales=nf(r.get("sales")),
                    partner=partner,
                    partner_name=r.get("partner_name", "(No Partner)"),
                    revenue_usd=nf(r.get("our_rev_usd")),
                    payout_usd=nf(r.get("payout_usd")),
                    profit_usd=nf(r.get("profit_usd")),
                )
            )

        ReefTransaction.objects.bulk_create(objs, batch_size=500)

    return len(df)


# ---------------------------------------------------
# PUSH TO PERFORMANCE
# ---------------------------------------------------

def push_reef_to_performance(date_from, date_to):
    advertiser = Advertiser.objects.filter(name=ADVERTISER_NAME).first()
    qs = ReefTransaction.objects.filter(
        order_date__gte=date_from,
        order_date__lte=date_to
    )

    if not qs.exists():
        print("⚠️ No Reef transactions found")
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

        # Aggregate by user type (FTU or RTU)
        if r.user_type == "FTU":
            g["ftu_orders"] += r.orders
            g["ftu_sales"] += float(r.sales) * exchange_rate
            g["ftu_revenue"] += float(r.revenue_usd)
            g["ftu_payout"] += 0.0 if is_mb else float(r.payout_usd)
        else:  # RTU
            g["rtu_orders"] += r.orders
            g["rtu_sales"] += float(r.sales) * exchange_rate
            g["rtu_revenue"] += float(r.revenue_usd)
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

    print(f"✅ Aggregated {len(objs)} Reef performance rows.")
    return len(objs)
