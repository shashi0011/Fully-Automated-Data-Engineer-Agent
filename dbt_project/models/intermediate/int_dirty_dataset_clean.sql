-- Intermediate model that aggregates and transforms the cleaned dataset
-- This model will summarize sales data by product and city

with cleaned_data as (
    select
        order_id,
        customer_name,
        age,
        gender,
        product,
        price,
        quantity,
        order_date,
        city,
        payment_method,
        price * quantity as total_sales  -- Calculate total sales for each order
    from
        {{ ref('stg_dirty_dataset_clean') }}  -- Reference to the staging model
)

select
    product,
    city,
    sum(total_sales) as total_sales,
    count(distinct order_id) as total_orders,
    count(distinct customer_name) as total_customers
from
    cleaned_data
group by
    product,
    city
