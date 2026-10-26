-- Grain: one row per order. Items, payments and reviews are aggregated to this grain
-- FIRST; joining them raw would multiply rows (one order has many items and payments).
with orders as (
    select * from {{ source('silver', 'orders') }}
),

items as (
    select
        order_id,
        count(*)                    as item_count,
        count(distinct product_id)  as distinct_products,
        count(distinct seller_id)   as distinct_sellers,
        sum(price)                  as items_price,
        sum(freight_value)          as freight_total,
        sum(item_total)             as order_total
    from {{ source('silver', 'order_items') }}
    group by order_id
),

payments as (
    select
        order_id,
        count(*)                    as payment_count,
        sum(payment_value)          as payment_total,
        max(payment_installments)   as max_installments
    from {{ source('silver', 'payments') }}
    group by order_id
),

-- 547 orders carry more than one review; keep the most recent one so the grain holds.
latest_review as (
    select order_id, review_score, review_creation_date, has_comment
    from (
        select
            order_id,
            review_score,
            review_creation_date,
            has_comment,
            row_number() over (
                partition by order_id
                order by review_creation_date desc, review_id
            ) as rn
        from {{ source('silver', 'reviews') }}
    )
    where rn = 1
)

select
    o.order_id,

    -- foreign keys
    o.customer_id,
    cast(o.order_purchase_timestamp as date) as order_date,

    -- order attributes
    o.order_status,
    o.is_delivered,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    -- delivery metrics computed in silver
    o.delivery_days,
    o.delivery_delay_days,
    o.is_late,

    -- basket measures
    coalesce(i.item_count, 0) as item_count,
    i.distinct_products,
    i.distinct_sellers,
    i.items_price,
    i.freight_total,
    i.order_total,

    -- payment measures
    coalesce(p.payment_count, 0) as payment_count,
    p.payment_total,
    p.max_installments,

    -- review (nullable: 768 orders were never reviewed)
    r.review_score,
    r.has_comment as review_has_comment,

    -- data quality flags carried through from silver
    o.dq_delivered_without_date,
    o.dq_date_without_delivered_status
from orders o
left join items    i on i.order_id = o.order_id
left join payments p on p.order_id = o.order_id
left join latest_review r on r.order_id = o.order_id