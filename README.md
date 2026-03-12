# sap-data-engineering

Data engineering portfolio — SAP S/4HANA + HANA Cloud + Dagster + Pydantic

## Projet 1 — SAP Data Extractor

Pipeline d'extraction SAP S/4HANA sandbox vers SAP HANA Cloud via Dagster.
Validation par Pydantic v2 Data Contracts + 9 Asset Checks qualité.

### Résultats

| Asset | API SAP | Table HANA | Rows | Checks |
|-------|---------|------------|------|--------|
| business_partners | API_BUSINESS_PARTNER | SAP_RAW.BUSINESS_PARTNERS | 50 | 3 PASS |
| gl_accounts | API_GLACCOUNTINCHARTOFACCOUNTS_SRV | SAP_RAW.GL_ACCOUNTS | 50 | 3 PASS |
| journal_entry_items | API_JOURNALENTRYITEMBASIC_SRV | SAP_RAW.JOURNAL_ENTRY_ITEMS | 50 | 3 PASS |

### Stack

- **Orchestration** : Dagster (assets, asset_checks, resources)
- **Source** : SAP S/4HANA OData API — api.sap.com sandbox
- **Destination** : SAP HANA Cloud (BTP Trial, hana-free)
- **Validation** : Pydantic v2 Data Contracts
- **Qualité** : 9 Dagster Asset Checks (not_empty, no_null_keys, domain_checks)
- **Connexion** : hdbcli (SAP HANA Client for Python)

### Lancement

```bash
git clone https://github.com/Vanelfokamcode/sap-data-engineering
cd sap-data-engineering
python -m venv venv && source venv/bin/activate
pip install dagster dagster-webserver hdbcli pydantic requests python-dotenv
cp .env.example .env
export $(cat .env | xargs)
dagster dev -m project_1_extractor
```

### Structure

```
project_1_extractor/
├── assets/
│   ├── business_partners.py
│   ├── gl_accounts.py
│   └── journal_entry_items.py
├── checks/
│   ├── business_partners_checks.py
│   ├── gl_accounts_checks.py
│   └── journal_entry_items_checks.py
├── contracts.py
├── resources/hana_resource.py
└── __init__.py
```

## Projet 2 — SAP Financial Analytics (à venir)

dbt models FICO sur les données extraites par le Projet 1.
