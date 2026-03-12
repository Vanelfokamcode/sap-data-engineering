from dagster import asset_check, AssetCheckResult, AssetCheckSeverity
from project_1_extractor.resources.hana_resource import HanaCloudResource


@asset_check(asset="gl_accounts", name="gl_not_empty")
def gl_not_empty(hana: HanaCloudResource) -> AssetCheckResult:
    conn = hana.get_connection(); cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM SAP_RAW.GL_ACCOUNTS")
    count = cursor.fetchone()[0]; conn.close()
    return AssetCheckResult(
        passed=count > 0,
        description=f"{count} GL Accounts dans HANA",
        severity=AssetCheckSeverity.WARN,
    )


@asset_check(asset="gl_accounts", name="gl_no_null_account")
def gl_no_null_account(hana: HanaCloudResource) -> AssetCheckResult:
    conn = hana.get_connection(); cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM SAP_RAW.GL_ACCOUNTS WHERE GL_ACCOUNT IS NULL")
    nulls = cursor.fetchone()[0]; conn.close()
    return AssetCheckResult(
        passed=nulls == 0,
        description=f"{nulls} valeurs NULL sur GL_ACCOUNT",
        severity=AssetCheckSeverity.ERROR,
    )


@asset_check(asset="gl_accounts", name="gl_valid_chart_length")
def gl_valid_chart_length(hana: HanaCloudResource) -> AssetCheckResult:
    conn = hana.get_connection(); cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM SAP_RAW.GL_ACCOUNTS WHERE LENGTH(CHART_OF_ACCOUNTS) > 4")
    invalid = cursor.fetchone()[0]; conn.close()
    return AssetCheckResult(
        passed=invalid == 0,
        description=f"{invalid} chart codes > 4 chars",
        severity=AssetCheckSeverity.WARN,
    )
