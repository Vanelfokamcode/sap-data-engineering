-- Mart : Balance Sheet SAP simplifie
-- Filtre sur les comptes de bilan (IS_BALANCE_SHEET = true)

WITH journal_entries AS (
    SELECT * FROM {{ ref('stg_journal_entries') }}
),
gl_accounts AS (
    SELECT * FROM {{ ref('stg_gl_accounts') }}
    WHERE is_balance_sheet_account = 'X'
),
balance_lines AS (
    SELECT
        je.company_code,
        je.fiscal_year,
        je.gl_account,
        gl.gl_account_name,
        gl.account_group,
        je.currency_code,
        SUM(CASE WHEN je.debit_credit_code = 'S' THEN je.amount ELSE 0 END) AS total_debit,
        SUM(CASE WHEN je.debit_credit_code = 'H' THEN je.amount ELSE 0 END) AS total_credit,
        SUM(CASE WHEN je.debit_credit_code = 'S' THEN je.amount ELSE 0 END)
        - SUM(CASE WHEN je.debit_credit_code = 'H' THEN je.amount ELSE 0 END) AS net_balance
    FROM journal_entries je
    INNER JOIN gl_accounts gl ON je.gl_account = gl.gl_account
    GROUP BY
        je.company_code,
        je.fiscal_year,
        je.gl_account,
        gl.gl_account_name,
        gl.account_group,
        je.currency_code
)

SELECT
    company_code,
    fiscal_year,
    gl_account,
    gl_account_name,
    account_group,
    currency_code,
    total_debit,
    total_credit,
    net_balance,
    CASE WHEN net_balance >= 0 THEN 'ASSET' ELSE 'LIABILITY' END AS balance_type
FROM balance_lines
ORDER BY gl_account
