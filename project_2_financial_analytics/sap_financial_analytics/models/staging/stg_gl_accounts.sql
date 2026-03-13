SELECT
    GL_ACCOUNT          AS gl_account,
    CHART_OF_ACCOUNTS   AS chart_of_accounts,
    GL_ACCOUNT_NAME     AS gl_account_name,
    IS_BALANCE_SHEET    AS is_balance_sheet_account,
    GL_ACCOUNT_GROUP    AS account_group
FROM SAP_RAW.GL_ACCOUNTS
WHERE GL_ACCOUNT IS NOT NULL
