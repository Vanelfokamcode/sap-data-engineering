# sap-data-engineering

Portfolio Data Engineering — SAP S/4HANA + HANA Cloud + Dagster + dbt + Pydantic

## Projets

### Projet 1 — SAP Data Extractor ✓

Extraction SAP S/4HANA OData → SAP HANA Cloud via Dagster.
Validation Pydantic v2 Data Contracts + 9 Asset Checks qualité.

| Asset | API SAP | Table HANA | Rows | Checks |
|-------|---------|------------|------|--------|
| business_partners | API_BUSINESS_PARTNER | SAP_RAW.BUSINESS_PARTNERS | 50 | 3 PASS |
| gl_accounts | API_GLACCOUNTINCHARTOFACCOUNTS_SRV | SAP_RAW.GL_ACCOUNTS | 50 | 3 PASS |
| journal_entry_items | API_JOURNALENTRYITEMBASIC_SRV | SAP_RAW.JOURNAL_ENTRY_ITEMS | 50 | 3 PASS |

**Stack** : Dagster · hdbcli · Pydantic v2 · SAP HANA Cloud · Python 3.12

### Projet 2 — SAP Financial Analytics ✓

Transformation dbt sur SAP_RAW → models FICO analytiques.

| Model | Type | Rows | Tests |
|-------|------|------|-------|
| stg_journal_entries | view | 50 | PASS |
| stg_gl_accounts | view | 50 | PASS |
| fct_journal_entries | table | 50 | PASS |

**Stack** : dbt-core · dbt-sap-hana-cloud · SAP HANA Cloud

## Architecture globale

```
SAP S/4HANA OData API
        │
        ▼ Dagster (Projet 1)
   SAP_RAW (HANA Cloud)
   ├── BUSINESS_PARTNERS
   ├── GL_ACCOUNTS
   └── JOURNAL_ENTRY_ITEMS
        │
        ▼ dbt (Projet 2)
   SAP_STAGING_SAP_STAGING (views)
   ├── stg_journal_entries
   └── stg_gl_accounts
        │
        ▼
   SAP_STAGING_SAP_MART (tables)
   └── fct_journal_entries
```

## Lancement Projet 1

```bash
git clone https://github.com/Vanelfokamcode/sap-data-engineering
cd sap-data-engineering
python -m venv venv && source venv/bin/activate
pip install dagster dagster-webserver hdbcli pydantic requests python-dotenv
cp .env.example .env
export $(cat .env | xargs)
dagster dev -m project_1_extractor
```

## Lancement Projet 2

```bash
pip install dbt-core
pip install git+https://github.com/SAP-samples/dbt-sap-hana-cloud.git
cd project_2_financial_analytics/sap_financial_analytics
# Créer ~/.dbt/profiles.yml (voir project_2_financial_analytics/README.md)
dbt run && dbt test
```
