from dagster import Definitions, load_assets_from_modules
import os
from project_1_extractor.assets import business_partners, gl_accounts
from project_1_extractor.resources.hana_resource import HanaCloudResource

all_assets = load_assets_from_modules([business_partners, gl_accounts])

defs = Definitions(
    assets=all_assets,
    resources={
        "hana": HanaCloudResource(
            host=os.environ.get("HANA_HOST", ""),
            port=int(os.environ.get("HANA_PORT", "443")),
            user=os.environ.get("HANA_USER", ""),
            password=os.environ.get("HANA_PASSWORD", ""),
        ),
    },
)
