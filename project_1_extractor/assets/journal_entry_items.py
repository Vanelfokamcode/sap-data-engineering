import os, requests
from dagster import asset, AssetExecutionContext
from project_1_extractor.resources.hana_resource import HanaCloudResource
from pydantic import BaseModel, field_validator
from decimal import Decimal
from typing import Optional


class JournalEntryItemContract(BaseModel):
    company_code:   str
    fiscal_year:    str
    gl_account:     Optional[str] = None
    amount:         Optional[Decimal] = None
    currency:       Optional[str] = None
    dc_code:        Optional[str] = None

    @field_validator("company_code")
    @classmethod
    def valid_cc(cls, v):
        if len(v) > 4: raise ValueError(f"CompanyCode trop long: {v}")
        return v

    @field_validator("amount", mode="before")
    @classmethod
    def parse_amount(cls, v):
        if v is None or v == "": return None
        try: return Decimal(str(v))
        except: return None


def fetch_journal_entry_items(api_key, base_url):
    url = (
        f"{base_url}/API_JOURNALENTRYITEMBASIC_SRV"
        "/A_JournalEntryItemBasic"
        "?$top=50&$format=json&$filter=CompanyCode eq '1010'"
    )
    headers = {"APIKey": api_key, "Accept": "application/json"}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()["d"]["results"]


@asset(name="journal_entry_items", group_name="sap_raw",
       description="Journal Entry Items FICO — CompanyCode 1010")
def journal_entry_items_asset(context: AssetExecutionContext, hana: HanaCloudResource) -> None:
    api_key  = os.environ.get("SAP_API_KEY", "NOT_FOUND")
    base_url = os.environ.get("SAP_SANDBOX_URL", "NOT_FOUND")
    records  = fetch_journal_entry_items(api_key, base_url)
    context.log.info(f"  -> {len(records)} records bruts")

    valid_rows = []; errors = 0
    for i, r in enumerate(records):
        try:
            c = JournalEntryItemContract(
                company_code=r.get("CompanyCode", ""),
                fiscal_year=r.get("LedgerFiscalYear", ""),
                gl_account=r.get("GLAccount"),
                amount=r.get("AmountInCompanyCodeCurrency"),
                currency=r.get("CompanyCodeCurrency"),
                dc_code=r.get("DebitCreditCode"),
            )
            valid_rows.append((
                i + 1,
                c.company_code,
                c.fiscal_year,
                c.gl_account,
                float(c.amount) if c.amount is not None else None,
                c.currency,
                c.dc_code,
            ))
        except Exception as e:
            errors += 1
            context.log.warning(f"Invalide row {i}: {e}")

    context.log.info(f"  -> {len(valid_rows)} valides / {errors} rejetes")

    conn = hana.get_connection(); cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE SAP_RAW.JOURNAL_ENTRY_ITEMS (
                ROW_ID      INTEGER      NOT NULL,
                COMPANY_CODE NVARCHAR(4) NOT NULL,
                FISCAL_YEAR  NVARCHAR(4) NOT NULL,
                GL_ACCOUNT   NVARCHAR(10),
                AMOUNT       DOUBLE,
                CURRENCY     NVARCHAR(5),
                DC_CODE      NVARCHAR(1),
                PRIMARY KEY (ROW_ID)
            )
        """)
        context.log.info("Table JOURNAL_ENTRY_ITEMS creee")
    except Exception as e:
        if "duplicate table" not in str(e).lower(): raise
        context.log.info("Table existe deja — ok")

    upsert = """UPSERT SAP_RAW.JOURNAL_ENTRY_ITEMS
        (ROW_ID, COMPANY_CODE, FISCAL_YEAR, GL_ACCOUNT, AMOUNT, CURRENCY, DC_CODE)
        VALUES (?,?,?,?,?,?,?) WITH PRIMARY KEY"""
    cursor.executemany(upsert, valid_rows)
    conn.commit(); conn.close()
    context.log.info(f"  -> {len(valid_rows)} rows dans SAP_RAW.JOURNAL_ENTRY_ITEMS")
