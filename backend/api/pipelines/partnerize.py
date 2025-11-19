import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'backend.settings')
django.setup()

from api.models import PartnerizeConversion
import uuid
import base64
import requests
import json
import pandas as pd
from datetime import datetime, timedelta

APP_KEY = "XHBaFY3jOY"
API_KEY = "9QQPhhOW"
PUBLISHER_ID = "1011l405470"
BASE_URL_V1 = "https://api.partnerize.com/reporting/report_publisher/publisher"

def make_auth_header(app_key, api_key):
    token = f"{app_key}:{api_key}"
    b64 = base64.b64encode(token.encode("utf-8")).decode("utf-8")
    return {
        "Authorization": f"Basic {b64}",
        "Accept": "application/json"
    }

def get_v1_conversion_report(start_dt, end_dt):
    url = f"{BASE_URL_V1}/{PUBLISHER_ID}/conversion.json"
    headers = make_auth_header(APP_KEY, API_KEY)
    params = {
        "start_date": start_dt,
        "end_date": end_dt
    }
    resp = requests.get(url, headers=headers, params=params)
    print("V1 Status:", resp.status_code)
    print("V1 Response:", resp.text[:500])
    resp.raise_for_status()
    return resp.json()

def extract_conversions(report):
    conversions = report.get("conversions", [])
    rows = []

    for conv in conversions:
        data = conv.get("conversion_data", {})
        if not data:
            continue

        voucher = None
        items = data.get("conversion_items", [])
        if items:
            first_item = items[0]
            vouchers = first_item.get("voucher_codes", [])
            if vouchers:
                voucher = vouchers[0].get("voucher_code")

        row = {
            "conversion_id": data.get("conversion_id"),
            "campaign_title": data.get("campaign_title"),
            "conversion_time": data.get("conversion_time"),
            "country": data.get("country"),
            "total_order_value": data.get("conversion_value", {}).get("value"),
            "total_commission": data.get("conversion_value", {}).get("publisher_commission"),
            "conversion_status": data.get("conversion_value", {}).get("conversion_status"),
            "voucher": voucher,
            "first_time_user": data.get("meta_data", {}).get("first_time_transaction")
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    return df


def load_partnerize_data_to_db():
    end = datetime.utcnow()
    start = end - timedelta(days=90)

    start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end.strftime("%Y-%m-%dT%H:%M:%SZ")

    report_v1 = get_v1_conversion_report(start_str, end_str)
    if not report_v1:
        print("⚠️ No data in report_v1.")
        return

    df = extract_conversions(report_v1)
    if df.empty:
        print("⚠️ No conversions extracted.")
        return

    created_count = 0
    for _, row in df.iterrows():
        try:
            obj = PartnerizeConversion(
                uuid=uuid.uuid4(),
                conversion_id=row["conversion_id"],
                campaign_title=row["campaign_title"],
                conversion_time=row["conversion_time"],
                country=row["country"],
                total_order_value=row["total_order_value"],
                total_commission=row["total_commission"],
                conversion_status=row["conversion_status"],
                voucher=row["voucher"],
                first_time_user=row["first_time_user"]
            )
            obj.save()
            created_count += 1
        except Exception as e:
            print(f"❌ Error saving row: {e}")

    print(f"✅ Loaded {created_count} Partnerize conversions.")


if __name__ == "__main__":
    load_partnerize_data_to_db()