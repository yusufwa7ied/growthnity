# backend/api/pipelines/noon_only.py

import pandas as pd
from datetime import date, datetime
from decimal import Decimal
from django.db import transaction
from django.conf import settings

from api.models import (
    Advertiser,
    NoonTransaction,
    CampaignPerformance,
    Partner,
    Coupon,
)

from api.pipelines.helpers import (
    store_raw_snapshot,
    enrich_df,
    nf,
    nz,
)
from api.services.s3_service import s3_service

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------

ADVERTISER_NAME = "Noon"
S3_GCC_KEY = "pipeline-data/noon_gcc.csv"
S3_EGYPT_KEY = "pipeline-data/noon_egypt.csv"

# Date cutoff for bracket-based logic
BRACKET_START_DATE = datetime(2025, 11, 18).date()

# Exchange rate for AED to USD (for pre-Nov 18 calculations)
AED_TO_USD = 0.27

# GCC Default Payouts (Post-Nov 18, Bracket-based)
GCC_DEFAULT_PAYOUTS = {
    "$1 Tier": 0.8,      # Bracket 1: <100 AED
    "$2 Tier": 1.6,      # Bracket 2: <150 AED
    "$3.5 Tier": 2.8,    # Bracket 3: <200 AED
    "$5 Tier": 4.0,      # Bracket 4: <400 AED
    "$7.5 Tier": 6.0,    # Bracket 5: >=400 AED
}

# GCC Special Payouts for Haron Ali & Mahmoud Houtan (Post-Nov 18 only)
NOON_GCC_SPECIAL_PAYOUTS = {
    "Haron Ali": {
        "$1 Tier": 0.95,
        "$2 Tier": 1.9,
        "$3.5 Tier": 3.25,
        "$5 Tier": 4.75,
        "$7.5 Tier": 7.0,
    },
    "Mahmoud Houtan": {
        "$1 Tier": 0.95,
        "$2 Tier": 1.9,
        "$3.5 Tier": 3.25,
        "$5 Tier": 4.75,
        "$7.5 Tier": 7.0,
    }
}

# Egypt Default Payouts (Post-Nov 18, Bracket-based)
EGYPT_DEFAULT_PAYOUTS = {
    "Bracket 1": 0.20,   # $4.75 - $14.25
    "Bracket 2": 0.55,   # $14.26 - $23.85
    "Bracket 3": 1.00,   # $23.86 - $37.24
    "Bracket 4": 1.70,   # $37.25 - $59.40
    "Bracket 5": 2.50,   # $59.41 - $72.00
    "Bracket 6": 3.25,   # $72.01 - $110.00
    "Bracket 7": 5.50,   # $110.01 & above
}


# ---------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------

def extract_tier_revenue(tier_str):
    """
    Extract revenue from GCC tier string.
    Examples: "$1 Tier" → 1.0, "$3.5 Tier" → 3.5, "$7.5 Tier" → 7.5
    """
    if pd.isna(tier_str) or not tier_str:
        return 0.0
    
    try:
        # Remove "$" and " Tier", convert to float
        value = tier_str.replace("$", "").replace(" Tier", "").strip()
        return float(value)
    except:
        return 0.0


def extract_bracket_number(bracket_str):
    """
    Extract bracket number from Egypt bracket string.
    Examples: "Bracket 2_$0.68" → "Bracket 2", "Bracket 7_$5.50" → "Bracket 7"
    """
    if pd.isna(bracket_str) or not bracket_str:
        return None
    
    try:
        # Split by "_" and take first part
        return bracket_str.split("_")[0].strip()
    except:
        return None


def extract_bracket_revenue(bracket_str):
    """
    Extract revenue from Egypt bracket string.
    Examples: "Bracket 2_$0.68" → 0.68, "Bracket 7_$5.50" → 5.50
    """
    if pd.isna(bracket_str) or not bracket_str:
        return 0.0
    
    try:
        # Split by "_$" and take second part
        value = bracket_str.split("_$")[1].strip()
        return float(value)
    except:
        return 0.0


def get_special_payout(partner_name, tier_str, is_gcc, order_date):
    """
    Check if partner gets special payout for this tier.
    Returns special payout amount or None if no special applies.
    """
    # No specials before Nov 18
    if order_date < BRACKET_START_DATE:
        return None
    
    # No specials for Egypt (currently)
    if not is_gcc:
        return None
    
    # Check if partner has special rates
    if partner_name not in NOON_GCC_SPECIAL_PAYOUTS:
        return None
    
    # Get special payout for this tier
    return NOON_GCC_SPECIAL_PAYOUTS[partner_name].get(tier_str)


def calculate_pre_nov18_revenue(ftu_value, rtu_value, currency="AED"):
    """
    Calculate revenue for pre-Nov 18 orders (percentage-based).
    FTU: 3% of sales + 3 AED
    RTU: 3% of sales
    """
    ftu_value = float(ftu_value or 0)
    rtu_value = float(rtu_value or 0)
    
    # Revenue calculation
    revenue_ftu = (ftu_value * 0.03) + (3 * AED_TO_USD if currency == "AED" else 0)
    revenue_rtu = rtu_value * 0.03
    
    total_revenue = revenue_ftu + revenue_rtu
    return round(total_revenue, 2)


def calculate_pre_nov18_payout(ftu_value, rtu_value, currency="AED"):
    """
    Calculate payout for pre-Nov 18 orders (percentage-based).
    FTU: 57.14% of revenue + 2 AED
    RTU: 60% of revenue
    """
    ftu_value = float(ftu_value or 0)
    rtu_value = float(rtu_value or 0)
    
    # First calculate revenue
    revenue_ftu = (ftu_value * 0.03) + (3 * AED_TO_USD if currency == "AED" else 0)
    revenue_rtu = rtu_value * 0.03
    
    # Then calculate payout
    payout_ftu = (revenue_ftu * 0.5714) + (2 * AED_TO_USD if currency == "AED" else 0)
    payout_rtu = revenue_rtu * 0.60
    
    total_payout = payout_ftu + payout_rtu
    return round(total_payout, 2)
