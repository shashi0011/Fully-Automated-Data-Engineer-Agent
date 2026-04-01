-- DataForge dbt Transformation Model
-- Transforms raw sales data into clean analytics-ready format

SELECT 
    product,
    region,
    CAST(date AS DATE) as date,
    CAST(sales AS DOUBLE) as sales,
    CAST(quantity AS INTEGER) as quantity,
    sales * quantity as total_revenue,
    CURRENT_TIMESTAMP as transformed_at
FROM sales_raw
WHERE product IS NOT NULL
  AND region IS NOT NULL
  AND sales > 0
