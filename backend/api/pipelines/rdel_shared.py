# backend/api/pipelines/rdel_shared.py
"""
Shared utilities for RDEL-format advertisers (Daham, Reef, El_Esaei_Kids, ElNahdi)
All these advertisers use the same CSV format:
- date | advertiser | coupon | country | orders | sales
"""

import pandas as pd
from api.models import Advertiser


def clean_rdel_format(df: pd.DataFrame, advertiser: Advertiser) -> pd.DataFrame:
    """
    Clean CSV data for RDEL-format advertisers.
    
    Expected columns:
    - date: transaction date (M/D/YYYY)
    - advertiser: advertiser name
    - coupon: coupon code
    - country: country code (KSA, UAE, KWT, QA, BAH, OMA, etc.)
    - orders: number of orders
    - sales: sales amount
    
    Args:
        df: Raw DataFrame from CSV
        advertiser: Advertiser model instance
        
    Returns:
        Cleaned DataFrame ready for enrichment
    """
    df = df.copy()
    
    # Rename columns to standard format
    df.rename(columns={
        "date": "created_at",
        "sales": "sales",
    }, inplace=True)
    
    # Parse date (M/D/YYYY format)
    df["created_at"] = pd.to_datetime(df["created_at"], format="%m/%d/%Y", errors="coerce")
    
    # Clean sales amount - remove commas and handle formatting
    df["sales"] = (
        df["sales"]
        .astype(str)
        .str.replace(",", "")
        .str.replace("%", "")  # Handle edge cases like "241.45%"
        .astype(float)
    )
    
    # Clean order count
    df["orders"] = df["orders"].astype(int)
    
    # Uppercase coupon codes for consistency
    df["coupon"] = df["coupon"].str.upper()
    
    # Country mapping - standardize to 3-letter ISO codes
    COUNTRY_MAP = {
        "KSA": "SAU",
        "UAE": "ARE",
        "QA": "QAT",
        "KW": "KWT",
        "OM": "OMN",
        "OMA": "OMN",
        "BH": "BHR",
        "BAH": "BHR",
    }
    df["country"] = df["country"].astype(str).str.upper().replace(COUNTRY_MAP)
    
    # Add standard fields
    df["rate_type"] = advertiser.rev_rate_type
    df["commission"] = 0.0  # Not provided in CSV
    
    # Create synthetic order_id (since each row is aggregated)
    df["order_id"] = df.apply(
        lambda row: f"{advertiser.name.upper()}_{row['created_at'].strftime('%Y%m%d')}_{row['coupon']}_{row['country']}",
        axis=1
    )
    
    # Default to RTU (we don't have user_type in data)
    df["user_type"] = "RTU"
    
    # Order count alias
    df["order_count"] = df["orders"]
    
    # Delivery status
    df["delivery_status"] = "delivered"
    
    # Defaults for enrichment
    df["partner_id"] = pd.NA
    df["partner_name"] = None
    df["partner_type"] = None
    df["advertiser_id"] = advertiser.id
    df["advertiser_name"] = advertiser.name
    df["currency"] = advertiser.currency
    
    return df
