from dagster import asset_check, AssetCheckResult, AssetCheckSeverity
from project_1_extractor.resources.hana_resource import HanaCloudResource


@asset_check(asset="business_partners", name="bp_not_empty")
def bp_not_empty(hana: HanaCloudResource) -> AssetCheckResult:
    conn = hana.get_connection(); cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM SAP_RAW.BUSINESS_PARTNERS")
    count = cursor.fetchone()[0]; conn.close()
    return AssetCheckResult(
        passed=count > 0,
        description=f"{count} Business Partners dans HANA",
        severity=AssetCheckSeverity.WARN,
    )


@asset_check(asset="business_partners", name="bp_no_null_bp_number")
def bp_no_null_bp_number(hana: HanaCloudResource) -> AssetCheckResult:
    conn = hana.get_connection(); cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM SAP_RAW.BUSINESS_PARTNERS WHERE BUSINESS_PARTNER IS NULL")
    nulls = cursor.fetchone()[0]; conn.close()
    return AssetCheckResult(
        passed=nulls == 0,
        description=f"{nulls} valeurs NULL sur BUSINESS_PARTNER",
        severity=AssetCheckSeverity.ERROR,
    )


@asset_check(asset="business_partners", name="bp_valid_category")
def bp_valid_category(hana: HanaCloudResource) -> AssetCheckResult:
    conn = hana.get_connection(); cursor = conn.cursor()
    cursor.execute("""SELECT COUNT(*) FROM SAP_RAW.BUSINESS_PARTNERS
        WHERE BP_CATEGORY NOT IN ('1','2','3')
        AND BP_CATEGORY IS NOT NULL""")
    invalid = cursor.fetchone()[0]; conn.close()
    return AssetCheckResult(
        passed=invalid == 0,
        description=f"{invalid} categories invalides (attendu: 1, 2 ou 3)",
        severity=AssetCheckSeverity.WARN,
    )
