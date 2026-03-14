from pathlib import Path
from dagster_dbt import DbtCliResource, dbt_assets

DBT_PROJECT_DIR = Path(__file__).parent.parent / "project_2_financial_analytics" / "sap_financial_analytics"
DBT_MANIFEST = DBT_PROJECT_DIR / "target" / "manifest.json"

@dbt_assets(manifest=DBT_MANIFEST)
def sap_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()
    yield from dbt.cli(["test"], context=context).stream()
