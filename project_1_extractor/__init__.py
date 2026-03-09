from dagster import Definitions, load_assets_from_modules
from dotenv import load_dotenv
import os
from project_1_extractor.assets import business_partners
from project_1_extractor.resources.hana_resource import HanaCloudResource
# Charger les variables d'environnement depuis .env
load_dotenv()
all_assets = load_assets_from_modules([business_partners])
defs = Definitions(
 assets=all_assets,
 resources={
 "hana": HanaCloudResource(
 host=os.environ["HANA_HOST"],
 port=int(os.environ["HANA_PORT"]),
 user=os.environ["HANA_USER"],
 password=os.environ["HANA_PASSWORD"],
 ),
 },
)

