-- Intermediate model for the finance dataset

-- Description: Intermediate model for data transformation and cleaning

WITH

int_data2_clean AS (
    SELECT
        ticker,
        company_name,
        sector,
        exchange,
        price,
        volume,
        market_cap,
        pe_ratio,
        dividend_yield,
        change_percent,
        high_52w,
        low_52w
    FROM
        {{ref('stg_data2_clean')}}
)

SELECT
    *
FROM
    int_data2_clean;