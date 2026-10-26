-- One row per product. Category names were already translated and flagged in silver.
select
    product_id,
    product_category_name,
    product_category_name_english,
    category_translation_source,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_photos_qty,
    product_name_length,
    product_description_length,
    -- Shipping cost correlates with volume, so expose it once here instead of
    -- recomputing it in every analysis.
    product_length_cm * product_height_cm * product_width_cm as product_volume_cm3
from {{ source('silver', 'products') }}