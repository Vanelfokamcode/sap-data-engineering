-- Mart : Variance Analysis SAP
-- Compare montants reels (journal entries) vs budget

WITH actuals AS (
    SELECT
        company_code,
        fiscal_year,
        gl_account,
        currency_code,
        SUM(amount) AS actual_amount
    FROM {{ ref('stg_journal_entries') }}
    GROUP BY company_code, fiscal_year, gl_account, currency_code
),

budget AS (
    SELECT
        company_code,
        fiscal_year,
        gl_account,
        budget_amount,
        currency_code
    FROM {{ ref('budget_gl_accounts') }}
)

SELECT
    COALESCE(a.company_code, b.company_code)   AS company_code,
    COALESCE(a.fiscal_year, b.fiscal_year)     AS fiscal_year,
    COALESCE(a.gl_account, b.gl_account)       AS gl_account,
    COALESCE(a.currency_code, b.currency_code) AS currency_code,
    COALESCE(b.budget_amount, 0)              AS budget_amount,
    COALESCE(a.actual_amount, 0)              AS actual_amount,
    COALESCE(a.actual_amount, 0) - COALESCE(b.budget_amount, 0) AS variance_amount,
    CASE
        WHEN COALESCE(b.budget_amount, 0) = 0 THEN NULL
        ELSE ROUND(
            (COALESCE(a.actual_amount, 0) - COALESCE(b.budget_amount, 0))
            / b.budget_amount * 100, 2
        )
    END AS variance_pct,
    CASE
        WHEN COALESCE(a.actual_amount, 0) > COALESCE(b.budget_amount, 0) THEN 'OVER_BUDGET'
        WHEN COALESCE(a.actual_amount, 0) < COALESCE(b.budget_amount, 0) THEN 'UNDER_BUDGET'
        ELSE 'ON_BUDGET'
    END AS budget_status

FROM actuals a
FULL OUTER JOIN budget b
    ON  a.company_code  = b.company_code
    AND a.fiscal_year   = b.fiscal_year
    AND a.gl_account    = b.gl_account
    AND a.currency_code = b.currency_code
