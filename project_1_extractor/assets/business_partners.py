import os
import requests
from datetime import datetime
from dagster import asset, AssetExecutionContext
from project_1_extractor.resources.hana_resource import HanaCloudResource


def parse_sap_date(sap_date):
    if not sap_date or not sap_date.startswith('/Date('):
        return None
    ts_ms = int(sap_date[6:-2])
    return datetime.utcfromtimestamp(ts_ms / 1000).strftime('%Y-%m-%d')


def fetch_all_business_partners(api_key, base_url):
    url = f"{base_url}/API_BUSINESS_PARTNER/A_BusinessPartner?$top=50&$format=json"
    headers = {"APIKey": api_key, "Accept": "application/json"}
    all_records = []

    while url:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()["d"]
        all_records.extend(data["results"])
        url = data.get("__next")

    return all_records


@asset(
    name="business_partners",
    group_name="sap_raw",
    description="Business Partners extraits de S/4HANA sandbox vers HANA Cloud",
)
def business_partners_asset(
    context: AssetExecutionContext,
    hana: HanaCloudResource,
) -> None:

    api_key = os.environ.get("SAP_API_KEY", "NOT_FOUND")
    base_url = os.environ.get("SAP_SANDBOX_URL", "NOT_FOUND")

    context.log.info(f"API_KEY present: {api_key != 'NOT_FOUND'} | premiers chars: {api_key[:8]}")
    context.log.info(f"BASE_URL: {base_url}")

    records = fetch_all_business_partners(api_key, base_url)
    context.log.info(f"  -> {len(records)} records extraits")

    rows = []
    for r in records:
        rows.append((
            r["BusinessPartner"],
            r.get("BusinessPartnerFullName"),
            r.get("BusinessPartnerCategory"),
            r.get("Country"),
            r.get("Language"),
            parse_sap_date(r.get("CreationDate")),
        ))

    conn = hana.get_connection()
    cursor = conn.cursor()

    upsert_sql = """
        UPSERT SAP_RAW.BUSINESS_PARTNERS
        (BUSINESS_PARTNER, FULL_NAME, BP_CATEGORY, COUNTRY, LANGUAGE, CREATION_DATE)
        VALUES (?,?,?,?,?,?)
        WITH PRIMARY KEY
    """
    cursor.executemany(upsert_sql, rows)
    conn.commit()
    conn.close()

    context.log.info(f"  -> {len(rows)} rows charges dans SAP_RAW.BUSINESS_PARTNERS")
