# backend/api/pipelines/noon_namshi.py

import pandas as pd
from datetime import date
from django.db import transaction
from django.conf import settings

from api.models import (
    Advertiser,
    NoonNamshiTransaction,
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
from api.models import PartnerPayout
from api.services.s3_service import s3_service

ADVERTISER_NAMES = {"Namshi"}  # Only Namshi - Noon orders excluded per user request
S3_CSV_KEY = settings.S3_PIPELINE_FILES["noon_namshi"]  # From settings.S3_PIPELINE_FILES

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

# Noon bracket-based payout structures by country
NOON_BRACKETS = {
    # Noon KSA/UAE (countries: SAU, ARE)
    # Brackets in AED, revenue/payout in USD
    "KSA_UAE": {
        "countries": ["SAU", "ARE"],
        "currency": "USD",
        "revenue": [
            (100, 1.0),    # <100 AED → $1
            (150, 2.0),    # 100-150 AED → $2
            (200, 3.5),    # 150-200 AED → $3.5
            (400, 5.0),    # 200-400 AED → $5
            (float('inf'), 7.5)  # ≥400 AED → $7
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
    },
    # Noon GCC (countries: QAT, KWT, OMN, BHR)
    # Brackets in AED, revenue/payout in USD
    "GCC": {
        "countries": ["QAT", "KWT", "OMN", "BHR"],
        "currency": "USD",
        "revenue": [
            (100, 3.0),    # <100 AED → $3
            (200, 6.0),    # 100-200 AED → $6
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
    },
    # Noon Egypt (country: EGY)
    "EGYPT": {
        "countries": ["EGY"],
        "currency": "USD",
        "revenue": [
            (14.25, 0.30),
            (23.85, 0.75),
            (37.24, 1.30),
            (59.40, 2.20),
            (72.00, 3.25),
            (110.00, 4.25),
            (float('inf'), 7.0)
        ],
        "default": [
            (14.25, 0.20),
            (23.85, 0.55),
            (37.24, 1.0),
            (59.40, 1.70),
            (72.00, 2.50),
            (110.00, 3.25),
            (float('inf'), 5.50)
        ],
        "special": [
            (14.25, 0.25),
            (23.85, 0.65),
            (37.24, 1.10),
            (59.40, 2.0),
            (72.00, 2.80),
            (110.00, 3.0),
            (float('inf'), 6.25)
        ]
    },
    "Namshi": {  # Using percentage-based, keep existing logic
        "use_percentage": True
    }
}

def get_bracket_config(country):
    """
    Determine which bracket configuration to use based on country.
    """
    for config_name, config in NOON_BRACKETS.items():
        if config_name == "Namshi":
            continue
        if country in config.get("countries", []):
            return config
    # Default to KSA/UAE if country not found
    return NOON_BRACKETS["KSA_UAE"]

def get_bracket_amount(order_value_aed, brackets):
    """
    Given an order value in AED and a list of (threshold, amount) tuples,
    return the appropriate fixed amount.
    """
    for threshold, amount in brackets:
        if order_value_aed < threshold:
            return amount
    return brackets[-1][1]  # Return last bracket if nothing matches

def calculate_noon_payouts(df, advertiser):
    """
    Calculate revenue and payouts for Noon based on bracket structure.
    For Namshi, fall back to percentage-based calculation.
    Bracket rules apply to ALL Noon orders (including historical data).
    
    IMPORTANT: For Noon orders:
    - PartnerPayout table is ONLY used as a boolean flag (exists/doesn't exist)
    - The ftu_payout, rtu_payout, and fixed_bonus values are IGNORED
    - Only the bracket amounts (from NOON_BRACKETS dict) are used
    """
    
    if advertiser.name == "Namshi":
        # Use existing percentage-based logic for Namshi
        return resolve_payouts_with_history(advertiser, df)
    
    # For Noon, apply bracket-based logic to ALL orders
    if df.empty:
        return df
    
    bracket_results = []
    for idx, row in df.iterrows():
        # Get country to determine bracket config
        country = row.get("country", "SAU")
        bracket_config = get_bracket_config(country)
        
        # Get order value in original currency
        order_value = float(row.get("sales", 0))
        
        # For Egypt, order value is already in USD
        # For others, it's in AED
        if bracket_config["currency"] == "AED":
            # Convert to USD using advertiser exchange rate
            exchange_rate = float(advertiser.exchange_rate or 0.27)
            order_value_for_bracket = order_value  # Keep in AED for bracket lookup
        else:
            # Egypt - already in USD
            order_value_for_bracket = order_value
        
        # Calculate revenue per order based on bracket
        revenue_per_order = get_bracket_amount(order_value_for_bracket, bracket_config["revenue"])
        
        # Check if partner has special payout
        # NOTE: For ALL Noon orders, PartnerPayout table is ONLY used as a flag.
        # The ftu_payout/rtu_payout/fixed_bonus values are IGNORED.
        # We only check: "Does the record exist?" → Yes = special brackets, No = default brackets
        partner_name = row.get("partner_name")
        partner = Partner.objects.filter(name=partner_name).first() if partner_name else None
        has_special = False
        
        if partner:
            special_payout = PartnerPayout.objects.filter(
                advertiser=advertiser,
                partner=partner
            ).first()
            has_special = special_payout is not None  # Just checking existence, not reading values
        
        # Use special or default bracket based on flag
        payout_brackets = bracket_config["special"] if has_special else bracket_config["default"]
        payout_per_order = get_bracket_amount(order_value_for_bracket, payout_brackets)
        
        # Calculate totals
        orders = int(row.get("orders", 0))
        our_rev = revenue_per_order * orders  # Already in USD
        payout = payout_per_order * orders    # Already in USD
        
        # Update row
        row_dict = row.to_dict()
        row_dict["our_rev"] = our_rev
        row_dict["payout"] = payout
        row_dict["profit"] = our_rev - payout
        row_dict["payout_usd"] = payout  # Already in USD
        row_dict["profit_usd"] = our_rev - payout
        
        # Set rates based on user type for compatibility
        user_type = row.get("user_type", "")
        if user_type == "FTU":
            row_dict["ftu_rate"] = revenue_per_order
        elif user_type == "RTU":
            row_dict["rtu_rate"] = revenue_per_order
        
        bracket_results.append(row_dict)
    
    # Return results as DataFrame
    if bracket_results:
        return pd.DataFrame(bracket_results)
    
    return df

def run(date_from: date, date_to: date):
    print(f"🚀 Running Noon/Namshi pipeline {date_from} → {date_to}")
    print("⚠️  NOTE: Noon orders are EXCLUDED from this pipeline")
    raw_df = fetch_raw_data()
    
    # Filter out Noon orders completely
    raw_df = raw_df[raw_df["Advertiser"].astype(str).str.strip() != "Noon"]
    print(f"📊 Filtered to {len(raw_df)} rows (Noon excluded)")
    
    # Store raw snapshot per advertiser
    for adv_name in ADVERTISER_NAMES:
        sub_df = raw_df[raw_df["Advertiser"].astype(str).str.strip() == adv_name]
        if sub_df.empty:
            continue
        advertiser = Advertiser.objects.filter(name=adv_name).first()
        if advertiser is None:
            print(f"⚠️ Warning: Advertiser '{adv_name}' not found. Skipping raw snapshot for this advertiser.")
            continue
        store_raw_snapshot(advertiser, sub_df, date_from, date_to, source="noon_namshi_csv")

    clean_df = clean_noon_namshi(raw_df)

    # resolve payouts per-advertiser (rows can mix Noon/Namshi)
    # enrich each advertiser separately to handle duplicate coupon codes
    final_rows = []
    for adv_name in ADVERTISER_NAMES:
        # Get rows for this advertiser
        adv_rows = clean_df[clean_df["advertiser_name"] == adv_name]
        if adv_rows.empty:
            continue
            
        advertiser = Advertiser.objects.filter(name=adv_name).first()
        if advertiser is None:
            # if advertiser record doesn't exist yet, skip safely
            continue
            
        # Enrich with advertiser-specific coupon lookup
        enriched = enrich_df(adv_rows, advertiser=advertiser)
        
        # Use bracket-based calculation for Noon, percentage for Namshi
        payout_df = calculate_noon_payouts(enriched, advertiser)
        final_rows.append(payout_df)

    if not final_rows:
        print("⚠️ Nothing to process.")
        return 0

    merged = pd.concat(final_rows, ignore_index=True)
    
    # For Namshi, compute_final_metrics calculates payout from rates
    # For Noon, brackets already calculated payout in calculate_noon_payouts
    # Process each advertiser separately since they use different logic
    final_dfs = []
    for adv_name in merged["advertiser_name"].unique():
        adv_mask = merged["advertiser_name"] == adv_name
        adv_df = merged[adv_mask].copy()
        
        if adv_name == "Namshi":
            from api.pipelines.helpers import compute_final_metrics
            advertiser = Advertiser.objects.get(name="Namshi")
            adv_df = compute_final_metrics(adv_df, advertiser)
        # Noon already has payout calculated in brackets
        
        final_dfs.append(adv_df)
    
    final_df = pd.concat(final_dfs, ignore_index=True)

    count = save_final_rows(final_df, date_from, date_to)
    push_to_performance(date_from, date_to)
    print(f"✅ Noon/Namshi pipeline inserted {count} rows.")
    return count


def fetch_raw_data() -> pd.DataFrame:
    print("📄 Loading Noon/Namshi CSV from S3...")
    df = s3_service.read_csv_to_df(S3_CSV_KEY)
    print(f"✅ Loaded {len(df)} rows")
    return df


def clean_noon_namshi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Input columns:
      Order Date | Advertiser | Country | Coupon Code | Total orders | NON-PAYABLE Orders
      | Total Order Value | FTU Orders | FTU Order Values | RTU Orders | RTU Order Value | Platform

    We split each source row into up to 2 rows (FTU and/or RTU), carrying the
    *per-segment* 'orders' and 'sales' to align with the rest of the system.
    """
    # normalize names
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

    # cast/clean basics
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["advertiser_name"] = df["advertiser_name"].astype(str).str.strip()
    df["coupon"] = df["coupon"].astype(str).str.strip().str.upper()
    df["country"] = df["country"].astype(str).str.upper().replace(COUNTRY_MAP)

    for c in ["total_orders","nonpayable_orders","ftu_orders_src","rtu_orders_src"]:
        df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0).astype(int)

    for c in ["total_value","ftu_value","rtu_value"]:
        df[c] = pd.to_numeric(df.get(c, 0.0), errors="coerce").fillna(0.0)

    rows = []

    # Helper to emit one normalized row
    def emit_row(src, user_type, orders_col, value_col):
        orders = int(src[orders_col])
        if orders <= 0:
            return
        sales = float(src[value_col])

        rows.append({
            # identifiers / meta
            "order_id": 0,  # aggregated line, no single order ID
            "created_at": src["created_at"],
            "delivery_status": "delivered",
            "country": src["country"],
            "coupon": src["coupon"],
            "user_type": user_type,

            # enrichment placeholders
            "partner_id": pd.NA,
            "partner_name": None,
            "partner_type": None,

            # advertiser name from CSV (will be validated against coupon → enrich_df)
            "advertiser_name": src["advertiser_name"],

            # counts & money
            "orders": orders,
            "ftu_orders": orders if user_type == "FTU" else 0,
            "rtu_orders": orders if user_type == "RTU" else 0,
            "sales": sales,

            # client-side commission not provided here → 0; we compute `our_rev` later
            "commission": 0.0,

            # currency/type will be filled from Advertiser later (in save step we keep original)
            "currency": None,
            "rate_type": None,
        })

    for _, r in df.iterrows():
        emit_row(r, "FTU", "ftu_orders_src", "ftu_value")
        emit_row(r, "RTU", "rtu_orders_src", "rtu_value")

    out = pd.DataFrame(rows)

    # If coupon enrichment later overwrites advertiser_name, good.
    # Otherwise we keep CSV advertiser_name as fallback.
    return out


def save_final_rows(df: pd.DataFrame, date_from: date, date_to: date) -> int:
    if df.empty:
        NoonNamshiTransaction.objects.filter(
            created_date__gte=date_from,
            created_date__lte=date_to,
        ).delete()
        return 0

    with transaction.atomic():
        NoonNamshiTransaction.objects.filter(
            created_date__gte=date_from,
            created_date__lte=date_to,
        ).delete()

        objs = []
        for r in df.to_dict(orient="records"):
            # pull currency/rate_type from Advertiser at save time
            adv = Advertiser.objects.filter(name=r.get("advertiser_name")).first()
            currency = getattr(adv, "currency", "AED") if adv else "AED"
            rate_type = getattr(adv, "rev_rate_type", "percent") if adv else "percent"
            
            # Lookup partner by ID if available
            partner_id = r.get("partner_id")
            partner = None
            if partner_id and not pd.isna(partner_id):
                partner = Partner.objects.filter(id=int(partner_id)).first()

            objs.append(
                NoonNamshiTransaction(
                    order_id=0,
                    created_date=r.get("created_at"),
                    delivery_status="delivered",
                    country=r.get("country"),
                    coupon=r.get("coupon"),
                    user_type=r.get("user_type"),
                    partner=partner,
                    partner_name=r.get("partner_name"),
                    partner_type=r.get("partner_type"),
                    advertiser_name=r.get("advertiser_name") or (adv.name if adv else ""),
                    currency=currency,
                    rate_type=rate_type,

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

        NoonNamshiTransaction.objects.bulk_create(objs, batch_size=2000)

    return len(df)


def push_to_performance(date_from: date, date_to: date):
    qs = NoonNamshiTransaction.objects.filter(
        created_date__date__gte=date_from,
        created_date__date__lte=date_to
    )
    if not qs.exists():
        print("⚠️ No Noon/Namshi rows to aggregate.")
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
                "ftu_sales": 0.0,
                "rtu_sales": 0.0,
                "ftu_revenue": 0.0,  # use our_rev
                "rtu_revenue": 0.0,
                "ftu_payout": 0.0,
                "rtu_payout": 0.0,
            }

        g = groups[key]
        
        # Get advertiser exchange rate for this row
        advertiser = Advertiser.objects.filter(name=r.advertiser_name).first()
        exchange_rate = float(advertiser.exchange_rate or 1.0) if advertiser else 1.0
        
        # For Noon: revenue/payout already in USD (from brackets), only convert sales
        # For Namshi: everything needs conversion from AED to USD
        is_noon = r.advertiser_name == "Noon"
        
        if r.user_type == "FTU":
            g["ftu_orders"] += r.orders
            g["ftu_sales"] += float(r.sales) * exchange_rate
            # Revenue and payout: only convert if NOT Noon (Noon brackets already in USD)
            g["ftu_revenue"] += float(r.our_rev) if is_noon else float(r.our_rev) * exchange_rate
            g["ftu_payout"] += float(r.payout) if is_noon else float(r.payout) * exchange_rate
        elif r.user_type == "RTU":
            g["rtu_orders"] += r.orders
            g["rtu_sales"] += float(r.sales) * exchange_rate
            # Revenue and payout: only convert if NOT Noon (Noon brackets already in USD)
            g["rtu_revenue"] += float(r.our_rev) if is_noon else float(r.our_rev) * exchange_rate
            g["rtu_payout"] += float(r.payout) if is_noon else float(r.payout) * exchange_rate

    with transaction.atomic():
        # delete only for Noon + Namshi advertisers in range
        for adv_name in ADVERTISER_NAMES:
            adv = Advertiser.objects.filter(name=adv_name).first()
            if adv:
                CampaignPerformance.objects.filter(
                    advertiser=adv,
                    date__gte=date_from,
                    date__lte=date_to
                ).delete()

        objs = []
        for _, g in groups.items():
            adv = Advertiser.objects.filter(name=g["advertiser_name"]).first()
            partner = Partner.objects.filter(name=g["partner_name"]).first() if g["partner_name"] else None
            coupon_obj = Coupon.objects.filter(code=g["coupon"]).first()

            objs.append(
                CampaignPerformance(
                    date=g["date"],
                    advertiser=adv,
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