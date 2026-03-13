SELECT
    ROW_ID          AS journal_entry_item_id,
    COMPANY_CODE    AS company_code,
    FISCAL_YEAR     AS fiscal_year,
    GL_ACCOUNT      AS gl_account,
    CAST(AMOUNT AS DOUBLE) AS amount,
    CURRENCY        AS currency_code,
    DC_CODE         AS debit_credit_code
FROM SAP_RAW.JOURNAL_ENTRY_ITEMS
WHERE COMPANY_CODE IS NOT NULL
