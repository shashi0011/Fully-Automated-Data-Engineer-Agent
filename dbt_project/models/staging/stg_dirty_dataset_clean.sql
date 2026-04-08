-- Staging model for the dirty_dataset_clean
-- This model cleans the raw dataset and prepares the data for further transformation

with raw_data as (
    select
        order_id,
        customer_name,
        age,
        gender,
        product,
        cast(price as double) as price,  -- Convert price to double for monetary calculations
        quantity,
        -- Standardizing date format from various formats to 'YYYY-MM-DD'
        case
            when order_date like '%/%/%' then date_format(order_date, '%Y/%m/%d')
            when order_date like '%-%' then date_format(order_date, '%Y-%m-%d')
            else NULL
        end as order_date,
        city,
        payment_method
    from
        {{ ref('dirty_dataset_raw') }}  -- Reference to the raw dataset
)

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
    payment_method
from
    raw_data
where
    order_date is not null  -- Filter out records with null order dates
