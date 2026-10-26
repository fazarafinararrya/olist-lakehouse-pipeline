-- One row per customer_id (Olist issues a new customer_id per order; customer_unique_id
-- is the person). Location is enriched from the zip-prefix table built in silver.
with customers as (
    select * from {{ source('silver', 'customers') }}
),

geo as (
    select * from {{ source('silver', 'geolocation_zip') }}
)

select
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    g.latitude                      as customer_latitude,
    g.longitude                     as customer_longitude,
    g.zip_code_prefix is not null   as has_geolocation
from customers c
left join geo g
    on g.zip_code_prefix = c.customer_zip_code_prefix