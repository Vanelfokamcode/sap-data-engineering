from dagster import asset_check, AssetCheckResult, AssetCheckSeverity
from project_1_extractor.resources.hana_resource import HanaCloudResource


@asset_check(asset="journal_entry_items", name="je_not_empty")
def je_not_empty(hana: HanaCloudResource) -> AssetCheckResult:
    conn = hana.get_connection(); cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM SAP_RAW.JOURNAL_ENTRY_ITEMS")
    count = cursor.fetchone()[0]; conn.close()
    return AssetCheckResult(
        passed=count > 0,
        description=f"{count} Journal Entry Items dans HANA",
        severity=AssetCheckSeverity.WARN,
    )


@asset_check(asset="journal_entry_items", name="je_no_null_company")
def je_no_null_company(hana: HanaCloudResource) -> AssetCheckResult:
    conn = hana.get_connection(); cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM SAP_RAW.JOURNAL_ENTRY_ITEMS WHERE COMPANY_CODE IS NULL")
    nulls = cursor.fetchone()[0]; conn.close()
    return AssetCheckResult(
        passed=nulls == 0,
        description=f"{nulls} valeurs NULL sur COMPANY_CODE",
        severity=AssetCheckSeverity.ERROR,
    )


@asset_check(asset="journal_entry_items", name="je_valid_dc_code")
def je_valid_dc_code(hana: HanaCloudResource) -> AssetCheckResult:
    conn = hana.get_connection(); cursor = conn.cursor()
    cursor.execute("""SELECT COUNT(*) FROM SAP_RAW.JOURNAL_ENTRY_ITEMS
        WHERE DC_CODE IS NOT NULL AND DC_CODE NOT IN ('H', 'S')""")
    invalid = cursor.fetchone()[0]; conn.close()
    return AssetCheckResult(
        passed=invalid == 0,
        description=f"{invalid} DC_CODE invalides (attendu: H, S ou NULL)",
        severity=AssetCheckSeverity.WARN,
    )
