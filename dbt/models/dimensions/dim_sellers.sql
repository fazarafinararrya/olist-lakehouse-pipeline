-- One row per seller, with the same zip-prefix enrichment as dim_customers.
with sellers as (
    select * from {{ source('silver', 'sellers') }}
),

geo as (
    select * from {{ source('silver', 'geolocation_zip') }}
)

select
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    g.latitude                      as seller_latitude,
    g.longitude                     as seller_longitude,
    g.zip_code_prefix is not null   as has_geolocation
from sellers s
left join geo g
    on g.zip_code_prefix = s.seller_zip_code_prefix