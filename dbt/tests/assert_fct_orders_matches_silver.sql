-- Reconciliation: gold must not lose or invent orders relative to silver.
-- A singular test passes when it returns zero rows.
with silver_count as (
    select count(*) as n from {{ source('silver', 'orders') }}
),

gold_count as (
    select count(*) as n from {{ ref('fct_orders') }}
)

select
    s.n as silver_orders,
    g.n as gold_orders
from silver_count s
cross join gold_count g
where s.n != g.n