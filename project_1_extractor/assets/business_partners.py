import os, requests
from datetime import datetime
from dagster import asset, AssetExecutionContext
from project_1_extractor.resources.hana_resource import HanaCloudResource
from project_1_extractor.contracts import BusinessPartnerContract
from pydantic import ValidationError


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
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        data = r.json()["d"]
        all_records.extend(data["results"])
        url = data.get("__next")
    return all_records


@asset(name="business_partners", group_name="sap_raw",
       description="Business Partners extraits de S/4HANA sandbox vers HANA Cloud")
def business_partners_asset(context: AssetExecutionContext, hana: HanaCloudResource) -> None:

    api_key  = os.environ.get("SAP_API_KEY", "NOT_FOUND")
    base_url = os.environ.get("SAP_SANDBOX_URL", "NOT_FOUND")
    context.log.info(f"API_KEY present: {api_key != 'NOT_FOUND'}")

    records = fetch_all_business_partners(api_key, base_url)
    context.log.info(f"  -> {len(records)} records bruts")

    valid_rows = []; errors = 0
    for r in records:
        try:
            contract = BusinessPartnerContract(
                business_partner=r.get("BusinessPartner", ""),
                full_name=r.get("BusinessPartnerFullName"),
                bp_category=r.get("BusinessPartnerCategory"),
                country=r.get("Country"),
                language=r.get("Language"),
                creation_date=parse_sap_date(r.get("CreationDate")),
            )
            valid_rows.append((
                contract.business_partner, contract.full_name,
                contract.bp_category, contract.country,
                contract.language, contract.creation_date,
            ))
        except ValidationError as e:
            errors += 1
            context.log.warning(f"Record invalide: {e.errors()[0]['msg']}")

    context.log.info(f"  -> {len(valid_rows)} valides / {errors} rejetes")

    conn = hana.get_connection()
    cursor = conn.cursor()

    # HANA ne supporte pas CREATE TABLE IF NOT EXISTS — on gere avec try/except
    try:
        cursor.execute("""
            CREATE TABLE SAP_RAW.BUSINESS_PARTNERS (
                BUSINESS_PARTNER  NVARCHAR(10)  NOT NULL,
                FULL_NAME         NVARCHAR(81),
                BP_CATEGORY       NVARCHAR(1),
                COUNTRY           NVARCHAR(3),
                LANGUAGE          NVARCHAR(2),
                CREATION_DATE     DATE,
                _EXTRACTED_AT     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (BUSINESS_PARTNER)
            )
        """)
        context.log.info("Table BUSINESS_PARTNERS creee")
    except Exception as e:
        if "duplicate table name" in str(e).lower() or "existing index" in str(e).lower():
            context.log.info("Table BUSINESS_PARTNERS existe deja — ok")
        else:
            raise

    upsert_sql = """UPSERT SAP_RAW.BUSINESS_PARTNERS
        (BUSINESS_PARTNER, FULL_NAME, BP_CATEGORY, COUNTRY, LANGUAGE, CREATION_DATE)
        VALUES (?,?,?,?,?,?) WITH PRIMARY KEY"""
    cursor.executemany(upsert_sql, valid_rows)
    conn.commit()
    conn.close()
    context.log.info(f"  -> {len(valid_rows)} rows charges dans SAP_RAW.BUSINESS_PARTNERS")
