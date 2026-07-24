-- Mart model for the finance dataset

-- Description: Mart model for creating analytical views on finance data

WITH

mart_data2_clean AS (
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
        {{ref('int_data2_clean')}}
)

SELECT
    *
FROM
    mart_data2_clean;