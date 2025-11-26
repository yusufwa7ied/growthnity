# backend/api/pipelines/springrose.py

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date
from django.db import transaction
from api.models import Advertiser, SpringRoseTransaction, CampaignPerformance, Partner, Coupon
from api.pipelines.helpers import store_raw_snapshot, enrich_df, resolve_payouts, compute_final_metrics, nf, nz

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------
ADVERTISER_NAME = "Spring Rose"
SOURCE_URL = "https://moga-dev.com/orders-sys-dashboard"

# ---------------------------------------------------
# MAIN RUN FUNCTION
# ---------------------------------------------------
def run(date_from: date, date_to: date):
    advertiser = Advertiser.objects.get(name=ADVERTISER_NAME)
    print(f"🚀 Running SpringRose pipeline {date_from} → {date_to}")

    # 1. FETCH RAW
    raw_df = fetch_raw_data()
    print("🔍 RAW DF HEAD:")
    print(raw_df.head(10))

    # 2. STORE SNAPSHOT
    store_raw_snapshot(advertiser, raw_df, date_from, date_to, source="springrose_web")

    # 3. CLEAN
    clean_df = clean_springrose(raw_df, advertiser)
    print("🧹 CLEAN DF HEAD:")
    print(clean_df.head(10))

    # 4. ENRICH
    enriched_df = enrich_df(clean_df, advertiser=advertiser)
    print("🔍 ENRICHED DF HEAD:")
    print(enriched_df.head(10))

    # 5. RESOLVE PAYOUTS
    payout_df = resolve_payouts(advertiser, enriched_df)
    print("💰 PAYOUT DF HEAD:")
    print(payout_df.head(10))

    # 6. FINAL METRICS
    final_df = compute_final_metrics(payout_df, advertiser)
    print("🔍 FINAL DF HEAD:")
    print(final_df.head(10))

    # 7. SAVE RESULTS
    count = save_final_rows(advertiser, final_df, date_from, date_to)
    push_springrose_to_performance(date_from, date_to)
    print(f"✅ SpringRose pipeline inserted {count} rows.")
    return count

# ---------------------------------------------------
# FETCH RAW
# ---------------------------------------------------
def fetch_raw_data() -> pd.DataFrame:
    print("🌐 Fetching SpringRose data...")
    response = requests.get(SOURCE_URL)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table")
    headers = [th.text.strip() for th in table.find_all("th")]

    data = []
    for row in table.find_all("tr")[1:]:
        cols = [td.text.strip() for td in row.find_all("td")]
        if cols:
            data.append(cols)

    df = pd.DataFrame(data, columns=headers)
    df = df.drop(columns=["Customer", "Branch", "City"], errors="ignore")

    df["Coupon Code"] = df["Coupon Code"].astype(str).str.strip().str.upper()

    # ✅ Filter by affiliate coupon codes
    coupon_codes = [
        "5SM","ACC","ALCOUPON","ALM","CAE","FAZA","GURU","JOOJ","KT","ND888",
        "REZEEM","SA7SA7","TOULIN","WADI","WADY","WAFII","WAFY","ZEEM",
        "SR1","SR2","SR3","SR4","SR5","SR6","SR7","SR8","SR9","SR10",
        "SR11","SR12","SR13","SR14","SR15","SR16","SR17","SR18","SR19","SR20",
        "SR21","SR22","SR23","SR24","SR25","SR26","SR27","SR28","SR29","SR30",
        "SR31","SR32","SR33","SR34","SR35","SR36","SR37","SR38","SR39","SR40",
        "SR41","SR42","SR43","SR44","SR45","SR46","SR47","SR48","SR49","SR50",
        "SS1","SS2","SS3","SS4","SS5","SS6","SS7","SS8","SS9","SS10",
        "SS11","SS12","SS13","SS14","SS15","SS16","SS17","SS18","SS19","SS20",
        "SS21","SS22","SS23","SS24","SS25","SS26","SS27","SS28","SS29","SS30",
        "SS31","SS32","SS33","SS34","SS35","SS36","SS37","SS38","SS39","SS40",
        "SS41","SS42","SS43","SS44","SS45","SS46","SS47","SS48","SS49","SS50"
    ]
    df = df[df["Coupon Code"].isin(coupon_codes)]
    df = df.reset_index(drop=True)
    print(f"✅ Loaded {len(df)} valid SpringRose rows.")
    return df

# ---------------------------------------------------
# CLEAN / NORMALIZE
# ---------------------------------------------------
def clean_springrose(df: pd.DataFrame, advertiser: Advertiser) -> pd.DataFrame:
    print("🧹 Cleaning SpringRose data...")

    df = df.rename(columns={
        "# Order ID": "order_id",
        "Created At": "created_at",
        "Coupon Code": "coupon",
        "Total Price": "sales",
        "Status": "delivery_status",
    })
    df["order_id"] = df["order_id"].astype(str).str.replace("#", "").str.strip()

    # ✅ Clean sales numeric (remove Arabic symbols, currency)
    df["sales"] = df["sales"].str.replace("ر.س", "", regex=False)
    df["sales"] = pd.to_numeric(df["sales"].str.replace(",", "").str.strip(), errors="coerce").fillna(0.0)

    # ✅ Standard fields
    df["user_type"] = "RTU"  # SpringRose only has returning users for now
    df["orders"] = 1
    df["ftu_orders"] = 0
    df["rtu_orders"] = 1

    df["created_at"] = pd.to_datetime(df["created_at"].str.replace("\n", " "), errors="coerce")

    df["country"] = "SAU"
    df["partner_id"] = pd.NA
    df["partner_name"] = None
    df["partner_type"] = None
    df["advertiser_id"] = advertiser.id
    df["advertiser_name"] = advertiser.name

    df["currency"] = advertiser.currency
    df["rate_type"] = advertiser.rev_rate_type

    df["commission"] = 0.0  # from client, if any
    return df

# ---------------------------------------------------
# SAVE FINAL ROWS
# ---------------------------------------------------
def save_final_rows(advertiser: Advertiser, df: pd.DataFrame, date_from: date, date_to: date) -> int:
    if df.empty:
        SpringRoseTransaction.objects.filter(
            created_date__gte=date_from, created_date__lte=date_to
        ).delete()
        return 0

    with transaction.atomic():
        SpringRoseTransaction.objects.filter(
            created_date__gte=date_from, created_date__lte=date_to
        ).delete()

        objs = []
        for r in df.to_dict(orient="records"):
            objs.append(
                SpringRoseTransaction(
                    order_id=r["order_id"],
                    created_date=r.get("created_at"),
                    delivery_status=r.get("delivery_status", ""),
                    country=r.get("country"),
                    coupon=r.get("coupon"),
                    user_type=r.get("user_type"),
                    partner_name=r.get("partner_name"),
                    partner_type=r.get("partner_type"),
                    advertiser_name=r.get("advertiser_name"),
                    currency=advertiser.currency,
                    rate_type=advertiser.rev_rate_type,
                    sales=nf(r.get("sales")),
                    commission=nf(r.get("commission")),
                    our_rev=nf(r.get("our_rev")),
                    payout=nf(r.get("payout")),
                    profit=nf(r.get("profit")),
                    payout_usd=nf(r.get("payout_usd")),
                    profit_usd=nf(r.get("profit_usd")),
                )
            )
        SpringRoseTransaction.objects.bulk_create(objs, batch_size=2000)
    return len(df)

# ---------------------------------------------------
# PUSH TO PERFORMANCE
# ---------------------------------------------------
def push_springrose_to_performance(date_from, date_to):
    advertiser = Advertiser.objects.filter(name=ADVERTISER_NAME).first()
    qs = SpringRoseTransaction.objects.filter(
        created_date__date__gte=date_from,
        created_date__date__lte=date_to
    )

    if not qs.exists():
        print("⚠️ No SpringRoseTransaction rows found.")
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
                "ftu_orders": 0, "rtu_orders": 0,
                "ftu_sales": 0, "rtu_sales": 0,
                "ftu_revenue": 0, "rtu_revenue": 0,
                "ftu_payout": 0, "rtu_payout": 0,
            }
        g = groups[key]
        # Use advertiser's exchange rate for USD conversion
        exchange_rate = float(advertiser.exchange_rate or 1.0)
        if r.user_type == "RTU":
            g["rtu_orders"] += 1
            g["rtu_sales"] += float(r.sales) * exchange_rate
            g["rtu_revenue"] += float(r.our_rev) * exchange_rate
            g["rtu_payout"] += float(r.payout) * exchange_rate

    with transaction.atomic():
        CampaignPerformance.objects.filter(
            advertiser=advertiser, date__gte=date_from, date__lte=date_to
        ).delete()
        objs = []
        for key, g in groups.items():
            partner = Partner.objects.filter(name=g["partner_name"]).first() if g["partner_name"] else None
            coupon_obj = Coupon.objects.filter(code=g["coupon"]).first()
            objs.append(
                CampaignPerformance(
                    date=g["date"], advertiser=advertiser,
                    partner=partner, coupon=coupon_obj, geo=g["geo"],
                    ftu_orders=g["ftu_orders"], rtu_orders=g["rtu_orders"],
                    total_orders=g["ftu_orders"] + g["rtu_orders"],
                    ftu_sales=g["ftu_sales"], rtu_sales=g["rtu_sales"],
                    total_sales=g["ftu_sales"] + g["rtu_sales"],
                    ftu_revenue=g["ftu_revenue"], rtu_revenue=g["rtu_revenue"],
                    total_revenue=g["ftu_revenue"] + g["rtu_revenue"],
                    ftu_payout=g["ftu_payout"], rtu_payout=g["rtu_payout"],
                    total_payout=g["ftu_payout"] + g["rtu_payout"],
                )
            )
        CampaignPerformance.objects.bulk_create(objs, batch_size=2000)
    print(f"✅ Aggregated {len(objs)} performance rows.")
    return len(objs)