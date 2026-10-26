-- Grain: one row per item within an order. This is the finest level of detail in the
-- dataset, so revenue by product, seller or category is computed from here.
with items as (
    select * from {{ source('silver', 'order_items') }}
),

orders as (
    select * from {{ source('silver', 'orders') }}
)

select
    -- natural key of the grain
    i.order_id,
    i.order_item_id,

    -- foreign keys into the dimensions
    i.product_id,
    i.seller_id,
    o.customer_id,
    cast(o.order_purchase_timestamp as date) as order_date,

    -- degenerate dimensions: order attributes small enough to keep on the fact itself
    o.order_status,
    o.is_delivered,

    -- measures (additive: safe to SUM at any level)
    i.price,
    i.freight_value,
    i.item_total,

    i.shipping_limit_date
from items i
join orders o
    on o.order_id = i.order_id