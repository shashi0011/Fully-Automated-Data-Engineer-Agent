-- Final mart model for sales summary
-- This model provides a summary view of total sales by city and payment method

with sales_summary as (
    select
        city,
        payment_method,
        sum(price * quantity) as total_sales,
        count(distinct order_id) as total_orders
    from
        {{ ref('int_dirty_dataset_clean') }}  -- Reference to the intermediate model
    group by
        city,
        payment_method
)

select
    city,
    payment_method,
    total_sales,
    total_orders
from
    sales_summary
