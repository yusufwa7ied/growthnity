# backend/api/pipelines/noon_gcc_only.py
"""
Pipeline for processing Noon GCC orders (SAU, ARE, QAT, KWT, OMN, BHR).
Uses bracket-based payouts with date cutoff logic:
- Before Nov 1, 2025: Old percentage-based logic
- From Nov 1, 2025 onwards: New bracket-based logic
"""

import pandas as pd
from datetime import date, datetime
from django.db import transaction
from django.conf import settings

from api.models import (
    Advertiser,
    NoonGCCTransaction,
    CampaignPerformance,
    Partner,
    Coupon,
    PartnerPayout,
)

from api.pipelines.helpers import (
    store_raw_snapshot,
    enrich_df,
    nf,
    nz,
)
from api.services.s3_service import s3_service

ADVERTISER_NAME = "Noon_GCC"
S3_CSV_KEY = settings.S3_PIPELINE_FILES["noon_gcc"]

# Date cutoff for new bracket logic
BRACKET_START_DATE = datetime(2025, 11, 1).date()

# GCC countries
GCC_COUNTRIES = ["SAU", "ARE", "QAT", "KWT", "OMN", "BHR"]

COUNTRY_MAP = {
    "SA": "SAU",
    "AE": "ARE",
    "UAE": "ARE",
    "QA": "QAT",
    "KW": "KWT",
    "OM": "OMN",
    "BH": "BHR",
    # Lowercase variants (in case Excel has lowercase)
    "sa": "SAU",
    "ae": "ARE",
    "uae": "ARE",
    "qa": "QAT",
    "kw": "KWT",
    "om": "OMN",
    "bh": "BHR",
}

# New bracket structure (from Nov 1, 2025)
# KSA/UAE brackets (SAU, ARE)
KSA_UAE_BRACKETS = {
    "revenue": [
        (100, 1.17),          # <100 AED → $1.17
        (150, 2.25),          # 100-150 AED → $2.25
        (200, 3.50),          # 150-200 AED → $3.50
        (400, 5.85),          # 200-400 AED → $5.85
        (float('inf'), 7.50)  # ≥400 AED → $7.50
    ],
    "default": [
        (100, 0.8),
        (150, 1.6),
        (200, 2.8),
        (400, 4.0),
        (float('inf'), 6.0)
    ],
    "special": [
        (100, 0.95),
        (150, 1.9),
        (200, 3.25),
        (400, 4.75),
        (float('inf'), 7.0)
    ]
}

# Other GCC brackets (QAT, KWT, OMN, BHR)
OTHER_GCC_BRACKETS = {
    "revenue": [
        (100, 3.0),           # <100 AED → $3
        (200, 6.0),           # 100-200 AED → $6
        (float('inf'), 12.0)  # ≥200 AED → $12
    ],
    "default": [
        (100, 2.0),
        (200, 4.5),
        (float('inf'), 9.0)
    ],
    "special": [
        (100, 2.5),
        (200, 5.25),
        (float('inf'), 10.5)
    ]
}


def get_bracket_amount(order_value_aed, brackets):
    """Given an order value in AED, return the bracket amount in USD."""
    for threshold, amount in brackets:
        if order_value_aed < threshold:
            return amount
    return brackets[-1][1]


def get_bracket_config(country):
    """Get bracket configuration based on country."""
    if country in ["SAU", "ARE"]:
        return KSA_UAE_BRACKETS
    else:
        return OTHER_GCC_BRACKETS


def calculate_new_brackets(df, advertiser):
    """
    Calculate revenue and payouts using new bracket structure (Nov 1, 2025+).
    PartnerPayout table is used ONLY as a boolean flag for special rates.
    """
    if df.empty:
        return df
    
    results = []
    
    for idx, row in df.iterrows():
        country = row.get("country", "SAU")
        bracket_config = get_bracket_config(country)
        
        # Order value is in AED
        order_value_aed = float(row.get("sales", 0))
        orders = int(row.get("orders", 0))
        
        # Calculate revenue per order
        revenue_per_order = get_bracket_amount(order_value_aed / orders if orders > 0 else 0, bracket_config["revenue"])
        
        # Check partner type and special payout
        partner_name = row.get("partner_name")
        partner = Partner.objects.filter(name=partner_name).first() if partner_name else None
        partner_type = row.get("partner_type", "AFF")
        has_special = False
        
        if partner:
            special_payout = PartnerPayout.objects.filter(
                advertiser=advertiser,
                partner=partner
            ).first()
            has_special = special_payout is not None
        
        # Calculate totals
        our_rev = revenue_per_order * orders
        
        # Media Buyers (MB) get 100% of revenue as payout
        if partner_type == "MB":
            payout = our_rev
        else:
            # Use special or default bracket for AFF/INF
            payout_brackets = bracket_config["special"] if has_special else bracket_config["default"]
            payout_per_order = get_bracket_amount(order_value_aed / orders if orders > 0 else 0, payout_brackets)
            payout = payout_per_order * orders
        
        # Update row
        row_dict = row.to_dict()
        row_dict["our_rev"] = our_rev
        row_dict["payout"] = payout
        row_dict["profit"] = our_rev - payout
        row_dict["payout_usd"] = payout
        row_dict["profit_usd"] = our_rev - payout
        
        # Set rates for compatibility
        user_type = row.get("user_type", "")
        if user_type == "FTU":
            row_dict["ftu_rate"] = revenue_per_order
        elif user_type == "RTU":
            row_dict["rtu_rate"] = revenue_per_order
        
        results.append(row_dict)
    
    return pd.DataFrame(results) if results else df


def calculate_old_logic(df, advertiser):
    """
    Calculate using old percentage-based logic (before Nov 1, 2025).
    Uses PartnerPayout table for FTU/RTU rates.
    """
    from api.pipelines.helpers import resolve_payouts_with_history, compute_final_metrics
    
    if df.empty:
        return df
    
    # Use existing helper functions for old logic
    payout_df = resolve_payouts_with_history(advertiser, df)
    final_df = compute_final_metrics(payout_df, advertiser)
    
    return final_df


def run(date_from: date, date_to: date):
    """Main pipeline execution."""
    print(f"🚀 Running Noon GCC pipeline {date_from} → {date_to}")
    
    advertiser = Advertiser.objects.filter(name=ADVERTISER_NAME).first()
    if not advertiser:
        print(f"❌ Advertiser '{ADVERTISER_NAME}' not found")
        return 0
    
    # 1. Fetch raw data
    raw_df = fetch_raw_data()
    
    # 2. Filter to Noon orders only (case-insensitive)
    if "ADVERTISER" in raw_df.columns:
        raw_df = raw_df[raw_df["ADVERTISER"].astype(str).str.strip().str.lower() == "noon"]
    elif "Advertiser" in raw_df.columns:
        raw_df = raw_df[raw_df["Advertiser"].astype(str).str.strip().str.lower() == "noon"]
    
    print(f"📊 Filtered to {len(raw_df)} Noon GCC rows")
    
    if raw_df.empty:
        print("⚠️ No Noon GCC orders found")
        return 0
    
    # 3. Store raw snapshot
    store_raw_snapshot(advertiser, raw_df, date_from, date_to, source="noon_gcc_csv")
    
    # 4. Clean and normalize
    clean_df = clean_noon_gcc(raw_df)
    print(f"✅ Cleaned {len(clean_df)} rows")
    print(f"   Rows with valid created_at: {clean_df['created_at'].notna().sum()}")
    print(f"   Rows with NA created_at: {clean_df['created_at'].isna().sum()}")
    
    # 5. Enrich with partner/coupon mapping
    enriched_df = enrich_df(clean_df, advertiser=advertiser)
    print(f"   After enrich - Rows with valid created_at: {enriched_df['created_at'].notna().sum()}")
    print(f"   After enrich - Rows with NA created_at: {enriched_df['created_at'].isna().sum()}")
    
    # 6. Split by date and apply appropriate logic
    old_rows = []
    new_rows = []
    skipped_no_date = 0
    
    for idx, row in enriched_df.iterrows():
        order_date = row.get("created_at")
        if pd.isna(order_date):
            skipped_no_date += 1
            continue
        
        if isinstance(order_date, str):
            order_date = pd.to_datetime(order_date).date()
        elif hasattr(order_date, 'date'):
            order_date = order_date.date()
        
        if order_date < BRACKET_START_DATE:
            old_rows.append(row)
        else:
            new_rows.append(row)
    
    print(f"📊 Date split: {len(old_rows)} OLD (before Nov 1), {len(new_rows)} NEW (from Nov 1), {skipped_no_date} skipped (no date)")
    
    # Process old logic rows
    final_rows = []
    if old_rows:
        old_df = pd.DataFrame(old_rows)
        print(f"📊 Processing {len(old_df)} rows with OLD logic (before Nov 1)")
        old_final = calculate_old_logic(old_df, advertiser)
        print(f"✅ OLD logic produced {len(old_final)} final rows")
        final_rows.append(old_final)
    
    # Process new bracket rows
    if new_rows:
        new_df = pd.DataFrame(new_rows)
        print(f"📊 Processing {len(new_df)} rows with NEW bracket logic (from Nov 1)")
        new_final = calculate_new_brackets(new_df, advertiser)
        print(f"✅ NEW bracket logic produced {len(new_final)} final rows")
        final_rows.append(new_final)
    
    if not final_rows:
        print("⚠️ No rows to process")
        return 0
    
    final_df = pd.concat(final_rows, ignore_index=True)
    
    # 7. Save to database
    count = save_final_rows(advertiser, final_df, date_from, date_to)
    
    # 8. Aggregate to performance table
    push_to_performance(advertiser, date_from, date_to)
    
    print(f"✅ Noon GCC pipeline inserted {count} rows")
    return count


def fetch_raw_data() -> pd.DataFrame:
    """Fetch Noon GCC CSV from S3."""
    print("📄 Loading Noon GCC CSV from S3...")
    df = s3_service.read_csv_to_df(S3_CSV_KEY)
    print(f"✅ Loaded {len(df)} rows")
    return df


def clean_noon_gcc(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and normalize Noon GCC data."""
    df = df.rename(columns={
        "ORDER DATE": "created_at",
        "ADVERTISER": "advertiser_name",
        "COUNTRY": "country",
        "COUPON CODE": "coupon",
        "TIER": "tier_bracket",
        "TOTAL ORDERS": "total_orders",
        "NON-PAYABLE ORDERS": "nonpayable_orders",
        "TOTAL VALUE": "total_value",
        "FTU ORDERS": "ftu_orders_src",
        "FTU ORDER VALUE": "ftu_value",
        "RTU ORDERS": "rtu_orders_src",
        "RTU ORDER VALUE": "rtu_value",
        "PLATFORM": "platform",
    })

    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", format="mixed")
    df["advertiser_name"] = df["advertiser_name"].astype(str).str.strip()
    df["coupon"] = df["coupon"].astype(str).str.strip().str.upper()
    
    # Normalize country: strip, convert to uppercase, then map 2-letter to 3-letter codes
    df["country"] = df["country"].astype(str).str.strip().str.upper().replace(COUNTRY_MAP)
    
    # Filter: Keep only GCC countries, exclude Egypt
    df = df[df["country"].isin(GCC_COUNTRIES)]
    print(f"✅ Filtered to GCC countries only: {len(df)} rows remaining")

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
            "tier_bracket": src.get("tier_bracket", ""),
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
    """Save to NoonGCCTransaction table."""
    if df.empty:
        NoonGCCTransaction.objects.filter(
            order_date__gte=date_from,
            order_date__lte=date_to,
            country__in=GCC_COUNTRIES
        ).delete()
        return 0

    with transaction.atomic():
        NoonGCCTransaction.objects.filter(
            order_date__gte=date_from,
            order_date__lte=date_to,
            country__in=GCC_COUNTRIES
        ).delete()

        objs = []
        for r in df.to_dict(orient="records"):
            partner_id = r.get("partner_id")
            partner = None
            if partner_id and not pd.isna(partner_id):
                partner = Partner.objects.filter(id=int(partner_id)).first()

            # Create order_id from date and coupon
            order_date_val = r.get("created_at")
            if hasattr(order_date_val, 'date'):
                order_date_val = order_date_val.date()
            
            objs.append(
                NoonGCCTransaction(
                    order_id=f"noon_gcc_{order_date_val}_{r.get('coupon')}_{len(objs)}",
                    order_date=order_date_val,
                    advertiser_name="Noon_GCC",
                    is_gcc=True,
                    region="gcc",
                    platform=r.get("platform", ""),
                    country=r.get("country"),
                    coupon_code=r.get("coupon"),
                    tier_bracket=str(r.get("tier_bracket", "")).strip(),
                    total_orders=nz(r.get("orders")),
                    non_payable_orders=0,
                    payable_orders=nz(r.get("orders")),
                    total_value=nf(r.get("sales")),
                    ftu_orders=nz(r.get("ftu_orders")),
                    ftu_value=nf(r.get("sales")) if r.get("user_type") == "FTU" else 0,
                    rtu_orders=nz(r.get("rtu_orders")),
                    rtu_value=nf(r.get("sales")) if r.get("user_type") == "RTU" else 0,
                    partner=partner,
                    partner_name=r.get("partner_name"),
                    revenue_usd=nf(r.get("our_rev", 0)),
                    payout_usd=nf(r.get("payout")),
                    our_rev_usd=nf(r.get("our_rev", 0)),
                    profit_usd=nf(r.get("profit")),
                    user_type=r.get("user_type", ""),
                )
            )

        NoonGCCTransaction.objects.bulk_create(objs, batch_size=2000)

    return len(objs)


def push_to_performance(advertiser: Advertiser, date_from: date, date_to: date):
    """Aggregate to CampaignPerformance."""
    # Include both uppercase and lowercase country codes for backwards compatibility
    gcc_countries_all = GCC_COUNTRIES + [c.lower() for c in GCC_COUNTRIES] + ["sa", "ae"]
    
    qs = NoonGCCTransaction.objects.filter(
        order_date__gte=date_from,
        order_date__lte=date_to,
        country__in=gcc_countries_all
    )
    
    if not qs.exists():
        print("⚠️ No Noon GCC rows to aggregate")
        return 0

    groups = {}
    
    for r in qs:
        # Normalize country code to uppercase 3-letter format
        country_normalized = str(r.country).upper()
        if country_normalized in COUNTRY_MAP:
            country_normalized = COUNTRY_MAP[country_normalized]
        elif country_normalized not in GCC_COUNTRIES:
            # Skip if not a valid GCC country after normalization
            continue
            
        # Clean coupon code - strip whitespace and convert None to empty string
        coupon_code = str(r.coupon_code).strip() if r.coupon_code else ""
        
        key = (r.order_date, r.partner_name, coupon_code, country_normalized)
        
        if key not in groups:
            groups[key] = {
                "date": r.order_date,
                "partner_name": r.partner_name,
                "coupon": coupon_code,
                "geo": country_normalized,
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
        
        # Check if order date is before or after bracket start
        order_date = r.order_date
        is_new_bracket = order_date >= BRACKET_START_DATE
        
        # For new brackets, values already in USD
        # For old logic, need to convert from AED
        exchange_rate = 0.27 if not is_new_bracket else 1.0
        
        # Check if partner is Media Buyer (MB) - they should have zero payout in performance
        partner_obj = Partner.objects.filter(name=r.partner_name).first() if r.partner_name else None
        is_mb = partner_obj and partner_obj.partner_type == "MB"
        
        if r.user_type == "FTU":
            g["ftu_orders"] += r.ftu_orders
            g["ftu_sales"] += float(r.ftu_value) * 0.27  # Sales always in AED, convert to USD
            g["ftu_revenue"] += float(r.revenue_usd) * exchange_rate
            # MB partners: zero payout in performance (they add costs later)
            g["ftu_payout"] += 0.0 if is_mb else (float(r.payout_usd) * exchange_rate)
        elif r.user_type == "RTU":
            g["rtu_orders"] += r.rtu_orders
            g["rtu_sales"] += float(r.rtu_value) * 0.27
            g["rtu_revenue"] += float(r.revenue_usd) * exchange_rate
            # MB partners: zero payout in performance (they add costs later)
            g["rtu_payout"] += 0.0 if is_mb else (float(r.payout_usd) * exchange_rate)

    with transaction.atomic():
        # Delete ALL Noon_GCC performance records for the date range (including old Egypt data)
        CampaignPerformance.objects.filter(
            advertiser=advertiser,
            date__gte=date_from,
            date__lte=date_to
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

    print(f"✅ Aggregated {len(objs)} Noon GCC performance rows")
    return len(objs)