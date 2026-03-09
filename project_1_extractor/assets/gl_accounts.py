import os, requests
from dagster import asset, AssetExecutionContext
from project_1_extractor.resources.hana_resource import HanaCloudResource
from project_1_extractor.contracts import GLAccountContract
from pydantic import ValidationError


def fetch_gl_accounts(api_key, base_url):
    url = f"{base_url}/API_GLACCOUNTINCHARTOFACCOUNTS_SRV/A_GLAccountInChartOfAccounts?$top=50&$format=json"
    headers = {"APIKey": api_key, "Accept": "application/json"}
    all_records = []
    while url:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        data = r.json()["d"]
        all_records.extend(data["results"])
        url = data.get("__next")
    return all_records


@asset(name="gl_accounts", group_name="sap_raw",
       description="GL Accounts extraits de S/4HANA sandbox vers HANA Cloud")
def gl_accounts_asset(context: AssetExecutionContext, hana: HanaCloudResource) -> None:

    api_key  = os.environ.get("SAP_API_KEY", "NOT_FOUND")
    base_url = os.environ.get("SAP_SANDBOX_URL", "NOT_FOUND")

    records = fetch_gl_accounts(api_key, base_url)
    context.log.info(f"  -> {len(records)} records bruts")

    valid_rows = []; errors = 0
    for r in records:
        try:
            contract = GLAccountContract(
                chart_of_accounts=r.get("ChartOfAccounts", ""),
                gl_account=r.get("GLAccount", ""),
                gl_account_name=r.get("GLAccountName"),
                is_balance_sheet_account=r.get("IsBalanceSheetAccount", ""),
                gl_account_group=r.get("AccountGroup"),
            )
            valid_rows.append((
                contract.chart_of_accounts, contract.gl_account,
                contract.gl_account_name, contract.is_balance_sheet_account,
                contract.gl_account_group,
            ))
        except ValidationError as e:
            errors += 1
            context.log.warning(f"Invalide: {e.errors()[0]['msg']}")

    context.log.info(f"  -> {len(valid_rows)} valides / {errors} rejetes")

    conn = hana.get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            CREATE TABLE SAP_RAW.GL_ACCOUNTS (
                CHART_OF_ACCOUNTS  NVARCHAR(4)  NOT NULL,
                GL_ACCOUNT         NVARCHAR(10) NOT NULL,
                GL_ACCOUNT_NAME    NVARCHAR(50),
                IS_BALANCE_SHEET   BOOLEAN,
                GL_ACCOUNT_GROUP   NVARCHAR(4),
                PRIMARY KEY (CHART_OF_ACCOUNTS, GL_ACCOUNT)
            )
        """)
        context.log.info("Table GL_ACCOUNTS creee")
    except Exception as e:
        if "duplicate table name" in str(e).lower() or "existing index" in str(e).lower():
            context.log.info("Table GL_ACCOUNTS existe deja — ok")
        else:
            raise

    upsert = """UPSERT SAP_RAW.GL_ACCOUNTS
        (CHART_OF_ACCOUNTS, GL_ACCOUNT, GL_ACCOUNT_NAME, IS_BALANCE_SHEET, GL_ACCOUNT_GROUP)
        VALUES (?,?,?,?,?) WITH PRIMARY KEY"""
    cursor.executemany(upsert, valid_rows)
    conn.commit()
    conn.close()
    context.log.info(f"  -> {len(valid_rows)} rows charges dans SAP_RAW.GL_ACCOUNTS")
