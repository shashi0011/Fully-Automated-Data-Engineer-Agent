-- Staging model for the finance dataset

-- Description: Staging model for loading raw finance data

WITH

stg_data_raw AS (
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
        {{ref('data_raw')}}
)

SELECT
    *
FROM
    stg_data_raw;