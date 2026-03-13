WITH journal_entries AS (
    SELECT * FROM {{ ref('stg_journal_entries') }}
),
gl_accounts AS (
    SELECT * FROM {{ ref('stg_gl_accounts') }}
)
SELECT
    je.journal_entry_item_id,
    je.company_code,
    je.fiscal_year,
    je.gl_account,
    gl.gl_account_name,
    gl.is_balance_sheet_account,
    gl.account_group,
    je.amount,
    je.currency_code,
    je.debit_credit_code,
    CASE WHEN je.debit_credit_code = 'S' THEN je.amount ELSE 0 END AS debit_amount,
    CASE WHEN je.debit_credit_code = 'H' THEN je.amount ELSE 0 END AS credit_amount
FROM journal_entries je
LEFT JOIN gl_accounts gl ON je.gl_account = gl.gl_account
