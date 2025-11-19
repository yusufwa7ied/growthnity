# backend/api/pipelines/noon_namshi.py

import pandas as pd
from datetime import date
from django.db import transaction

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
    resolve_payouts,
    compute_final_metrics,
    nf,
    nz,
)

ADVERTISER_NAMES = {"Noon", "Namshi"}  # we’ll respect whichever appears per row
RAW_CSV = "/Users/yusuf/noon-namshi.csv"   # replace with your real path

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
    print(f"🚀 Running Noon/Namshi pipeline {date_from} → {date_to}")
    raw_df = fetch_raw_data()
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
    enriched = enrich_df(clean_df)

    # resolve payouts per-advertiser (rows can mix Noon/Namshi)
    final_rows = []
    for adv_name, chunk in enriched.groupby("advertiser_name", dropna=False):
        if adv_name not in ADVERTISER_NAMES:
            # keep future-proof: if coupon mapping sets a different adv name, skip silently
            continue
        advertiser = Advertiser.objects.filter(name=adv_name).first()
        if advertiser is None:
            # if advertiser record doesn’t exist yet, skip safely
            continue
        payout_df = resolve_payouts(advertiser, chunk)
        final_rows.append(payout_df)

    if not final_rows:
        print("⚠️ Nothing to process.")
        return 0

    merged = pd.concat(final_rows, ignore_index=True)
    final_df = compute_final_metrics(merged, advertiser)

    count = save_final_rows(final_df, date_from, date_to)
    push_to_performance(date_from, date_to)
    print(f"✅ Noon/Namshi pipeline inserted {count} rows.")
    return count


def fetch_raw_data() -> pd.DataFrame:
    print("📄 Loading Noon/Namshi CSV...")
    df = pd.read_csv(RAW_CSV)
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

            objs.append(
                NoonNamshiTransaction(
                    order_id=0,
                    created_date=r.get("created_at"),
                    delivery_status="delivered",
                    country=r.get("country"),
                    coupon=r.get("coupon"),
                    user_type=r.get("user_type"),
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