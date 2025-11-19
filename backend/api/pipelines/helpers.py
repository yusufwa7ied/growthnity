# backend/pipelines/helpers.py

import hashlib
from datetime import datetime, date
import json
import numpy as np
from decimal import Decimal
from datetime import datetime

import pandas as pd

from django.utils.timezone import make_aware

from api.models import (
    Advertiser,
    Coupon,
    PartnerPayout,
    RawAdvertiserRecord,
)



# --------------------------------------------
# UTILS
# --------------------------------------------

def hash_row(*values) -> str:
    """Stable row hash using any combination of fields."""
    concat = "|".join([str(v) for v in values])
    return hashlib.sha256(concat.encode()).hexdigest()


def nz(x):
    """Turn None, NaN, or empty into 0 (integer)."""
    try:
        return int(x) if x == x and x is not None else 0
    except:
        return 0


def nf(x):
    """Turn None, NaN, or empty into 0.0 (float)."""
    try:
        return float(x) if x == x and x is not None else 0.0
    except:
        return 0.0


# --------------------------------------------
# RAW STORAGE
# --------------------------------------------




def store_raw_snapshot(advertiser, df, date_from, date_to, source: str):
    """Save raw data JSON for auditing/replay."""

    if df.empty:
        return

    # Convert dataframe to JSON-safe dict
    safe_rows = json.loads(
        df.map(lambda x: float(x) if isinstance(x, Decimal) else x)
          .replace({np.nan: None}).to_json(orient="records")
    )

    RawAdvertiserRecord.objects.create(
        advertiser=advertiser,
        source=source,
        date_from=date_from,
        date_to=date_to,
        data={
            "date_from": str(date_from),
            "date_to": str(date_to),
            "rows": safe_rows,
        },
        date_fetched=make_aware(datetime.utcnow())
    )


# --------------------------------------------
# PARTNER & COUPON ENRICHMENT
# --------------------------------------------

def enrich_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Ensure columns exist (defensive)
    for col in ["partner_id","partner_name","partner_type","advertiser_id","advertiser_name","coupon"]:
        if col not in df.columns:
            df[col] = pd.NA

    df["coupon"] = df["coupon"].astype(str).str.strip().str.upper()

    coupons = pd.DataFrame(list(Coupon.objects.values(
        "code",
        "advertiser__id",
        "advertiser__name",
        "partner__id",
        "partner__name",
        "partner__partner_type",
    )))
    if coupons.empty:
        return df

    coupons = coupons.rename(columns={
        "code": "coupon",
        "advertiser__id": "adv_id",
        "advertiser__name": "advertiser_name_coupon",
        "partner__id": "coupon_partner_id",
        "partner__name": "coupon_partner_name",
        "partner__partner_type": "coupon_partner_type",
    })
    coupons["coupon"] = coupons["coupon"].astype(str).str.upper()

    # Merge without clobbering left columns
    df = df.merge(coupons, on="coupon", how="left")

    # Combine/cast
    df["partner_id"] = pd.to_numeric(df["partner_id"], errors="coerce").astype("Int64")
    df["coupon_partner_id"] = pd.to_numeric(df["coupon_partner_id"], errors="coerce").astype("Int64")

    df["partner_id"]   = df["coupon_partner_id"].combine_first(df["partner_id"])
    df["partner_name"] = df["coupon_partner_name"].combine_first(df["partner_name"])
    df["partner_type"] = df["coupon_partner_type"].combine_first(df["partner_type"])

    # Advertiser ALWAYS from coupon
    df["advertiser_id"]   = df["adv_id"]
    df["advertiser_name"] = df["advertiser_name_coupon"].combine_first(df["advertiser_name"])

    # Defaults
    df["partner_name"] = df["partner_name"].fillna("(No Partner)")
    df["partner_type"] = df["partner_type"].fillna("AFF")
    df["advertiser_name"] = df["advertiser_name"].fillna("(Unknown Advertiser)")

    # Cleanup helper cols
    df.drop(columns=[
        "coupon_partner_id","coupon_partner_name","coupon_partner_type",
        "adv_id","advertiser_name_coupon",
    ], inplace=True, errors="ignore")
    print
    return df

# --------------------------------------------
# PAYOUT RESOLUTION (FTU/RTU, percent/fixed)
# --------------------------------------------

def resolve_payouts(advertiser: Advertiser, df: pd.DataFrame) -> pd.DataFrame:
    """Attach correct payout logic per row (FTU & RTU), supporting:
       - percent payouts
       - fixed payouts
       - date-range based payouts
       - partner-specific or default payouts (3-level fallback)
       
    Payout Resolution Order:
    1. Partner-specific PartnerPayout (partner != NULL)
    2. Default PartnerPayout (partner = NULL)
    3. Advertiser default_ftu_payout / default_rtu_payout
    """

    # Load advertiser revenue rules
    adv = advertiser
    adv_ftu_rate = getattr(adv, "rev_ftu_rate", 0) or 0
    adv_rtu_rate = getattr(adv, "rev_rtu_rate", 0) or 0
    adv_rate_type = getattr(adv, "rev_rate_type", "percent") or "percent"
    adv_ftu_fixed_bonus = getattr(adv, "rev_ftu_fixed_bonus", None)
    adv_rtu_fixed_bonus = getattr(adv, "rev_rtu_fixed_bonus", None)
    
    # ✅ Load advertiser-level default payouts
    adv_default_ftu = getattr(adv, "default_ftu_payout", None)
    adv_default_rtu = getattr(adv, "default_rtu_payout", None)
    adv_default_ftu_bonus = getattr(adv, "default_ftu_fixed_bonus", None)
    adv_default_rtu_bonus = getattr(adv, "default_rtu_fixed_bonus", None)
    adv_default_rate_type = getattr(adv, "default_payout_rate_type", "percent") or "percent"

    payouts = pd.DataFrame(
        list(
            PartnerPayout.objects.filter(advertiser=advertiser).values(
                "partner__id",
                "ftu_payout",
                "rtu_payout",
                "ftu_fixed_bonus",
                "rtu_fixed_bonus",
                "exchange_rate",
                "currency",
                "rate_type",   # percent or fixed
                "condition",
                "start_date",
                "end_date",
            )
        )
    )

    if payouts.empty:
        # ✅ No PartnerPayout rules → use Advertiser defaults
        df["ftu_rate"] = float(adv_default_ftu) if adv_default_ftu else 0.0
        df["rtu_rate"] = float(adv_default_rtu) if adv_default_rtu else 0.0
        df["rate_type"] = adv_default_rate_type
        df["ftu_fixed_bonus"] = float(adv_default_ftu_bonus) if adv_default_ftu_bonus else 0.0
        df["rtu_fixed_bonus"] = float(adv_default_rtu_bonus) if adv_default_rtu_bonus else 0.0
        # Continue to revenue calculation
        df = df.copy()
        df["partner_id"] = pd.to_numeric(df.get("partner_id", pd.NA), errors="coerce").astype("Int64")

    payouts.rename(columns={"partner__id": "payout_partner_id"}, inplace=True)
    # ✅ Ensure integer alignment
    payouts["payout_partner_id"] = pd.to_numeric(payouts["payout_partner_id"], errors="coerce").astype("Int64")
    payouts["start_date"] = pd.to_datetime(payouts["start_date"], errors="coerce").dt.date
    payouts["end_date"] = pd.to_datetime(payouts["end_date"], errors="coerce").dt.date
    payouts["start_date"] = payouts["start_date"].fillna(date.min)
    payouts["end_date"] = payouts["end_date"].fillna(date.max)

    # ✅ Filter payouts by date range if provided
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

        payouts = payouts[
            payouts.apply(lambda p: any(
                (df["date"] >= p["start_date"]) & (df["date"] <= p["end_date"])
            ), axis=1)
        ]

    df = df.copy()
    df["partner_id"] = pd.to_numeric(df["partner_id"], errors="coerce").astype("Int64")

    # ✅ Split default vs partner-specific
    default_rules = payouts[payouts["payout_partner_id"].isna()]
    partner_rules = payouts[payouts["payout_partner_id"].notna()]

    # ✅ Merge partner-specific rules first
    df = df.merge(
        partner_rules,
        left_on="partner_id",
        right_on="payout_partner_id",
        how="left",
        suffixes=("", "_ps")
    )

    # ✅ Apply default PartnerPayout rule where partner rule is missing
    if not default_rules.empty:
        d = default_rules.iloc[0]
        df["ftu_payout"] = df["ftu_payout"].fillna(d["ftu_payout"])
        df["rtu_payout"] = df["rtu_payout"].fillna(d["rtu_payout"])
        df["rate_type"] = df["rate_type"].fillna(d["rate_type"])
        # Ensure fixed bonus columns exist before filling
        for col in ["ftu_fixed_bonus", "rtu_fixed_bonus"]:
            if col not in df.columns:
                df[col] = 0.0
        # Safely extract bonus values (handle NaN or missing)
        ftu_bonus_val = 0
        rtu_bonus_val = 0
        if not default_rules.empty:
            ftu_bonus_val = pd.to_numeric(d.get("ftu_fixed_bonus", 0), errors="coerce")
            rtu_bonus_val = pd.to_numeric(d.get("rtu_fixed_bonus", 0), errors="coerce")
            if pd.isna(ftu_bonus_val):
                ftu_bonus_val = 0
            if pd.isna(rtu_bonus_val):
                rtu_bonus_val = 0
        # Fill with safe numeric values
        df["ftu_fixed_bonus"] = df["ftu_fixed_bonus"].fillna(ftu_bonus_val)
        df["rtu_fixed_bonus"] = df["rtu_fixed_bonus"].fillna(rtu_bonus_val)
    
    # ✅ LEVEL 3: Apply Advertiser default payouts where no PartnerPayout exists
    for col in ["ftu_payout", "rtu_payout", "ftu_fixed_bonus", "rtu_fixed_bonus"]:
        if col not in df.columns:
            df[col] = pd.NA
    
    # Convert to numeric and handle NaN
    df["ftu_payout"] = pd.to_numeric(df["ftu_payout"], errors="coerce")
    df["rtu_payout"] = pd.to_numeric(df["rtu_payout"], errors="coerce")
    df["ftu_fixed_bonus"] = pd.to_numeric(df["ftu_fixed_bonus"], errors="coerce")
    df["rtu_fixed_bonus"] = pd.to_numeric(df["rtu_fixed_bonus"], errors="coerce")
    
    # Fill missing ftu/rtu payouts with advertiser defaults (if they exist)
    if adv_default_ftu is not None:
        df["ftu_payout"] = df["ftu_payout"].fillna(float(adv_default_ftu))
    else:
        df["ftu_payout"] = df["ftu_payout"].fillna(0.0)
    
    if adv_default_rtu is not None:
        df["rtu_payout"] = df["rtu_payout"].fillna(float(adv_default_rtu))
    else:
        df["rtu_payout"] = df["rtu_payout"].fillna(0.0)
    
    # Fill missing fixed bonuses with advertiser defaults
    if adv_default_ftu_bonus is not None:
        df["ftu_fixed_bonus"] = df["ftu_fixed_bonus"].fillna(float(adv_default_ftu_bonus))
    else:
        df["ftu_fixed_bonus"] = df["ftu_fixed_bonus"].fillna(0.0)
    
    if adv_default_rtu_bonus is not None:
        df["rtu_fixed_bonus"] = df["rtu_fixed_bonus"].fillna(float(adv_default_rtu_bonus))
    else:
        df["rtu_fixed_bonus"] = df["rtu_fixed_bonus"].fillna(0.0)
    
    # Apply advertiser default rate_type if still missing
    if "rate_type" not in df.columns:
        df["rate_type"] = adv_default_rate_type
    else:
        df["rate_type"] = df["rate_type"].fillna(adv_default_rate_type)

    # ✅ After rules applied, calculate numeric rates
    df["ftu_rate"] = pd.to_numeric(df["ftu_payout"], errors="coerce").fillna(0.0)
    df["rtu_rate"] = pd.to_numeric(df["rtu_payout"], errors="coerce").fillna(0.0)

    # ✅ Calculate our revenue using advertiser rates + fixed bonuses
    df["our_rev"] = 0.0

    ftu_mask = df["user_type"].astype(str).str.upper().eq("FTU")
    rtu_mask = df["user_type"].astype(str).str.upper().eq("RTU")

    if adv_rate_type == "percent":
        # Percent of sales + fixed bonus per order
        df.loc[ftu_mask, "our_rev"] = (
            pd.to_numeric(df.loc[ftu_mask, "sales"], errors="coerce").fillna(0.0)
            * (float(adv_ftu_rate) / 100.0)
        )
        df.loc[rtu_mask, "our_rev"] = (
            pd.to_numeric(df.loc[rtu_mask, "sales"], errors="coerce").fillna(0.0)
            * (float(adv_rtu_rate) / 100.0)
        )
        
        # ✅ Add advertiser fixed bonuses (e.g., Noon: 7% + 3 AED per FTU order)
        if adv_ftu_fixed_bonus:
            df.loc[ftu_mask, "our_rev"] += (
                pd.to_numeric(df.loc[ftu_mask, "orders"], errors="coerce").fillna(0.0)
                * float(adv_ftu_fixed_bonus)
            )
        if adv_rtu_fixed_bonus:
            df.loc[rtu_mask, "our_rev"] += (
                pd.to_numeric(df.loc[rtu_mask, "orders"], errors="coerce").fillna(0.0)
                * float(adv_rtu_fixed_bonus)
            )
    else:
        # Fixed per-order revenue
        df.loc[ftu_mask, "our_rev"] = (
            pd.to_numeric(df.loc[ftu_mask, "orders"], errors="coerce").fillna(0.0)
            * float(adv_ftu_rate)
        )
        df.loc[rtu_mask, "our_rev"] = (
            pd.to_numeric(df.loc[rtu_mask, "orders"], errors="coerce").fillna(0.0)
            * float(adv_rtu_rate)
        )

    return df


# --------------------------------------------
# FINAL METRICS (payout + profit)
# --------------------------------------------

def compute_final_metrics(df: pd.DataFrame, advertiser: Advertiser) -> pd.DataFrame:
    """
    Calculate payout & profit using the latest business rules.

    Rules:
      - "commission" is our revenue.
      - If partner_type == "MB": payout = commission and profit = 0.
      - Else (AFF/INF):
          * If rate_type == "percent":
              FTU row → payout = commission * (ftu_rate / 100)
              RTU row → payout = commission * (rtu_rate / 100)
          * If rate_type == "fixed":
              FTU row → payout = orders * ftu_rate
              RTU row → payout = orders * rtu_rate
    Also ensures backward-compat columns used downstream:
      - creates `total_payout` (alias of `payout`)
      - ensures `revenue` exists (fallback to `commission` if missing)
    """

    df = df.copy()
    
    # before using bonuses
    for col in ["ftu_fixed_bonus", "rtu_fixed_bonus"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df["orders"] = pd.to_numeric(df.get("orders", 1), errors="coerce").fillna(1.0)

    # --- Ensure required numeric columns ---
    if "orders" not in df.columns:
        df["orders"] = 1

    for col in ["commission", "orders", "ftu_payout", "rtu_payout", "ftu_rate", "rtu_rate"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # If ftu_rate/rtu_rate are missing, derive from ftu_payout/rtu_payout
    if "ftu_rate" not in df.columns and "ftu_payout" in df.columns:
        df["ftu_rate"] = pd.to_numeric(df["ftu_payout"], errors="coerce").fillna(0.0)
    else:
        df["ftu_rate"] = pd.to_numeric(df.get("ftu_rate", 0.0), errors="coerce").fillna(0.0)

    if "rtu_rate" not in df.columns and "rtu_payout" in df.columns:
        df["rtu_rate"] = pd.to_numeric(df["rtu_payout"], errors="coerce").fillna(0.0)
    else:
        df["rtu_rate"] = pd.to_numeric(df.get("rtu_rate", 0.0), errors="coerce").fillna(0.0)

    # Ensure rate_type exists
    if "rate_type" not in df.columns:
        df["rate_type"] = "percent"

    df["revenue"] = pd.to_numeric(df["commission"], errors="coerce").fillna(0.0)

    # Initialize payout/profit columns
    df["payout"] = 0.0
    df["profit"] = 0.0

    # --- MB logic ---
    mb_mask = df.get("partner_type", "").eq("MB")
    # Use our_rev if present, otherwise fall back to commission
    if "our_rev" in df.columns:
        mb_payout = pd.to_numeric(df.loc[mb_mask, "our_rev"], errors="coerce").fillna(0.0)
        mb_payout_fallback = pd.to_numeric(df.loc[mb_mask, "commission"], errors="coerce").fillna(0.0)
        # If our_rev is present and nonzero, use it, else use commission
        df.loc[mb_mask, "payout"] = np.where(
            df.loc[mb_mask, "our_rev"].notna(), mb_payout, mb_payout_fallback
        )
    else:
        df.loc[mb_mask, "payout"] = pd.to_numeric(df.loc[mb_mask, "commission"], errors="coerce").fillna(0.0)
    df.loc[mb_mask, "profit"] = 0.0  # per latest rule: MB keeps 0 profit

    # --- Non-MB logic (AFF/INF) ---
    non_mb_mask = ~mb_mask

    # Guarantee user_type values
    user_type = df.get("user_type", "").fillna("")

    # Percent rates
    percent_mask = non_mb_mask & df["rate_type"].astype(str).str.lower().eq("percent")
    ftu_mask = percent_mask & user_type.eq("FTU")
    rtu_mask = percent_mask & user_type.eq("RTU")

    # --- Custom logic for percent rates: prefer our_rev if present and nonzero, else commission ---
    def revenue_base(df_subset):
        # Use our_rev if present and nonzero, else commission
        if "our_rev" in df_subset.columns:
            our_rev = pd.to_numeric(df_subset["our_rev"], errors="coerce").fillna(0.0)
            commission = pd.to_numeric(df_subset["commission"], errors="coerce").fillna(0.0)
            # Use our_rev if >0, else commission
            return np.where(our_rev > 0.0, our_rev, commission)
        else:
            return pd.to_numeric(df_subset["commission"], errors="coerce").fillna(0.0)

    # Only apply to percent rows, and only for FTU/RTU (DrNutrition, Styli logic unaffected elsewhere)
    if ftu_mask.any():
        base = revenue_base(df.loc[ftu_mask])
        df.loc[ftu_mask, "payout"] = (
            base * (df.loc[ftu_mask, "ftu_rate"].astype(float) / 100.0)
            + df.loc[ftu_mask, "orders"].astype(float) * df.loc[ftu_mask, "ftu_fixed_bonus"].astype(float)
        )
    if rtu_mask.any():
        base = revenue_base(df.loc[rtu_mask])
        df.loc[rtu_mask, "payout"] = (
            base * (df.loc[rtu_mask, "rtu_rate"].astype(float) / 100.0)
            + df.loc[rtu_mask, "orders"].astype(float) * df.loc[rtu_mask, "rtu_fixed_bonus"].astype(float)
        )

    # Fixed rates (per-order)
    fixed_mask = non_mb_mask & df["rate_type"].astype(str).str.lower().eq("fixed")
    ftu_fixed = fixed_mask & user_type.eq("FTU")
    rtu_fixed = fixed_mask & user_type.eq("RTU")

    df.loc[ftu_fixed, "payout"] = (
        df.loc[ftu_fixed, "orders"].astype(float) * df.loc[ftu_fixed, "ftu_rate"].astype(float)
        + df.loc[ftu_fixed, "orders"].astype(float) * df.loc[ftu_fixed, "ftu_fixed_bonus"].astype(float)
    )
    df.loc[rtu_fixed, "payout"] = (
        df.loc[rtu_fixed, "orders"].astype(float) * df.loc[rtu_fixed, "rtu_rate"].astype(float)
        + df.loc[rtu_fixed, "orders"].astype(float) * df.loc[rtu_fixed, "rtu_fixed_bonus"].astype(float)
    )

    if "our_rev" in df.columns:
        our_rev = pd.to_numeric(df.loc[non_mb_mask, "our_rev"], errors="coerce").fillna(0.0)
        commission = pd.to_numeric(df.loc[non_mb_mask, "commission"], errors="coerce").fillna(0.0)
        base_rev = np.where(our_rev > 0, our_rev, commission)
    else:
        base_rev = pd.to_numeric(df.loc[non_mb_mask, "commission"], errors="coerce").fillna(0.0)

    df.loc[non_mb_mask, "profit"] = base_rev - pd.to_numeric(df.loc[non_mb_mask, "payout"], errors="coerce").fillna(0.0)

    # Backward-compat: total_payout used elsewhere
    df["total_payout"] = df["payout"]

    # Optional detailed columns similar to earlier version
    # (These are set to 0 by default unless percent path used)
    df["payout_ftu"] = 0.0
    df["payout_rtu"] = 0.0
    df.loc[ftu_mask, "payout_ftu"] = df.loc[ftu_mask, "payout"]
    df.loc[rtu_mask, "payout_rtu"] = df.loc[rtu_mask, "payout"]
    
    # ✅ USD conversion using advertiser's exchange_rate
    # Get exchange rate from advertiser (default to 1.0 if not set)
    exchange_rate = float(getattr(advertiser, "exchange_rate", None) or 1.0)
    
    if "our_rev" in df.columns:
        df["our_rev"] = pd.to_numeric(df["our_rev"], errors="coerce").fillna(0.0)
        df["our_rev_usd"] = df["our_rev"] * exchange_rate
    else:
        df["our_rev"] = 0.0
        df["our_rev_usd"] = 0.0
    
    df["payout_usd"] = pd.to_numeric(df["payout"], errors="coerce").fillna(0.0) * exchange_rate
    df["profit_usd"] = pd.to_numeric(df["profit"], errors="coerce").fillna(0.0) * exchange_rate

    return df


