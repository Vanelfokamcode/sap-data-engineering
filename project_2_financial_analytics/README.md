# Projet 2 — SAP Financial Analytics

Couche de transformation dbt sur les données SAP extraites par le Projet 1.
Source : SAP_RAW (HANA Cloud). Destination : SAP_STAGING + SAP_MART.

## Stack

- **dbt** : dbt-core + dbt-sap-hana-cloud (adapter officiel SAP, GitHub only)
- **Destination** : SAP HANA Cloud (BTP Trial)
- **Source** : SAP_RAW — tables extraites par Dagster (Projet 1)

## Installation

```bash
pip install dbt-core
pip install git+https://github.com/SAP-samples/dbt-sap-hana-cloud.git
```

Créer ~/.dbt/profiles.yml :

```yaml
sap_financial_analytics:
  target: dev
  outputs:
    dev:
      type: saphanacloud
      host: 
      port: "443"
      user: 
      password: 
      database: SAP_RAW
      schema: SAP_STAGING
      threads: 1
```

## Lancement

```bash
cd project_2_financial_analytics/sap_financial_analytics
dbt run     # crée views + table
dbt test    # lance tous les tests
```

## DAG

```
source: SAP_RAW.JOURNAL_ENTRY_ITEMS ──▶ stg_journal_entries ──▶
                                                                  fct_journal_entries
source: SAP_RAW.GL_ACCOUNTS ──────────▶ stg_gl_accounts ────────▶
```

## Résultats

| Model | Type | Schema HANA | Rows | Tests |
|-------|------|-------------|------|-------|
| stg_journal_entries | view | SAP_STAGING_SAP_STAGING | 50 | PASS |
| stg_gl_accounts | view | SAP_STAGING_SAP_STAGING | 50 | PASS |
| fct_journal_entries | table | SAP_STAGING_SAP_MART | 50 | PASS |

## Structure

```
sap_financial_analytics/
├── dbt_project.yml
└── models/
    ├── staging/
    │   ├── sources.yml          ← SAP_RAW déclaré
    │   ├── schema.yml           ← tests staging
    │   ├── stg_journal_entries.sql
    │   └── stg_gl_accounts.sql
    └── mart/
        ├── schema.yml           ← tests mart
        └── fct_journal_entries.sql
```

## Notes HANA

- Schema naming : dbt concatène profil schema + model schema
  → SAP_STAGING + SAP_MART = SAP_STAGING_SAP_MART
- HANA case-sensitive : guillemets doubles pour les noms en minuscules
- port doit être une string dans profiles.yml : port: "443"
