from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
import os
from project_1_extractor.assets import business_partners, gl_accounts, journal_entry_items
from project_1_extractor.checks import (
    business_partners_checks,
    gl_accounts_checks,
    journal_entry_items_checks,
)
from project_1_extractor.resources.hana_resource import HanaCloudResource

all_assets = load_assets_from_modules([business_partners, gl_accounts, journal_entry_items])

all_checks = load_asset_checks_from_modules([
    business_partners_checks,
    gl_accounts_checks,
    journal_entry_items_checks,
])

defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    resources={
        "hana": HanaCloudResource(
            host=os.environ.get("HANA_HOST", ""),
            port=int(os.environ.get("HANA_PORT", "443")),
            user=os.environ.get("HANA_USER", ""),
            password=os.environ.get("HANA_PASSWORD", ""),
        ),
    },
)
