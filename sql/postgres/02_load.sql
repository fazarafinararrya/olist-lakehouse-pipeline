-- Load the Olist CSV exports into the source schema.
-- COPY runs server-side, so these paths are inside the postgres container.
-- Order matters: parent tables first, because of the foreign keys.

\echo '--- customers'
COPY olist.customers FROM '/data/source/olist_customers_dataset.csv' WITH (FORMAT csv, HEADER true);

\echo '--- sellers'
COPY olist.sellers FROM '/data/source/olist_sellers_dataset.csv' WITH (FORMAT csv, HEADER true);

\echo '--- products'
COPY olist.products FROM '/data/source/olist_products_dataset.csv' WITH (FORMAT csv, HEADER true);

\echo '--- category translation'
COPY olist.product_category_name_translation
    FROM '/data/source/product_category_name_translation.csv' WITH (FORMAT csv, HEADER true);

\echo '--- geolocation (1M rows, takes a moment)'
COPY olist.geolocation FROM '/data/source/olist_geolocation_dataset.csv' WITH (FORMAT csv, HEADER true);

\echo '--- orders'
COPY olist.orders FROM '/data/source/olist_orders_dataset.csv' WITH (FORMAT csv, HEADER true);

\echo '--- order_items'
COPY olist.order_items FROM '/data/source/olist_order_items_dataset.csv' WITH (FORMAT csv, HEADER true);

\echo '--- order_payments'
COPY olist.order_payments FROM '/data/source/olist_order_payments_dataset.csv' WITH (FORMAT csv, HEADER true);

\echo '--- order_reviews (quoted fields contain newlines; COPY CSV handles them)'
COPY olist.order_reviews FROM '/data/source/olist_order_reviews_dataset.csv' WITH (FORMAT csv, HEADER true);

-- The translation export starts with a UTF-8 BOM, which COPY keeps as part of the first
-- value. Strip it so the category joins correctly.
UPDATE olist.product_category_name_translation
SET product_category_name = ltrim(product_category_name, chr(65279))
WHERE product_category_name LIKE chr(65279) || '%';

ANALYZE;

\echo '--- row counts (compare with docs/data_profile.md)'
SELECT 'customers'   AS table_name, count(*) FROM olist.customers
UNION ALL SELECT 'sellers',         count(*) FROM olist.sellers
UNION ALL SELECT 'products',        count(*) FROM olist.products
UNION ALL SELECT 'translation',     count(*) FROM olist.product_category_name_translation
UNION ALL SELECT 'geolocation',     count(*) FROM olist.geolocation
UNION ALL SELECT 'orders',          count(*) FROM olist.orders
UNION ALL SELECT 'order_items',     count(*) FROM olist.order_items
UNION ALL SELECT 'order_payments',  count(*) FROM olist.order_payments
UNION ALL SELECT 'order_reviews',   count(*) FROM olist.order_reviews
ORDER BY table_name;