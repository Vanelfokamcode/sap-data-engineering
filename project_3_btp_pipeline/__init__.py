from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource
import os

from project_1_extractor.assets import business_partners, gl_accounts, journal_entry_items
from project_1_extractor.resources.hana_resource import HanaCloudResource
from project_3_btp_pipeline.dbt_assets import sap_dbt_assets, DBT_PROJECT_DIR

extraction_assets = load_assets_from_modules([
    business_partners,
    gl_accounts,
    journal_entry_items,
])

defs = Definitions(
    assets=[*extraction_assets, sap_dbt_assets],
    resources={
        "hana": HanaCloudResource(
            host=os.environ.get("HANA_HOST", ""),
            port=int(os.environ.get("HANA_PORT", "443")),
            user=os.environ.get("HANA_USER", ""),
            password=os.environ.get("HANA_PASSWORD", ""),
        ),
        "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR),
    },
)
