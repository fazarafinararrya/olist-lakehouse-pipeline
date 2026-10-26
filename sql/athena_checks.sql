-- Sanity checks and analytics to run in the Athena console
-- (workgroup: olist, database: olist_silver).
-- Query 1 must agree with docs/data_profile.md and the data quality report.

-- 1. Row counts per table
SELECT 'orders' AS table_name, count(*) AS rows FROM olist_silver.orders
UNION ALL SELECT 'order_items', count(*) FROM olist_silver.order_items
UNION ALL SELECT 'payments',    count(*) FROM olist_silver.payments
UNION ALL SELECT 'reviews',     count(*) FROM olist_silver.reviews
UNION ALL SELECT 'customers',   count(*) FROM olist_silver.customers
UNION ALL SELECT 'sellers',     count(*) FROM olist_silver.sellers
UNION ALL SELECT 'products',    count(*) FROM olist_silver.products
ORDER BY table_name;

-- 2. Revenue by month (items + freight), delivered orders only
SELECT date_trunc('month', o.order_purchase_timestamp) AS month,
       count(DISTINCT o.order_id)                      AS orders,
       round(sum(i.item_total), 2)                     AS revenue
FROM olist_silver.orders o
JOIN olist_silver.order_items i ON i.order_id = o.order_id
WHERE o.is_delivered
GROUP BY 1
ORDER BY 1;

-- 3. Top 10 product categories by revenue
SELECT p.product_category_name_english AS category,
       count(*)                        AS items_sold,
       round(sum(i.item_total), 2)     AS revenue
FROM olist_silver.order_items i
JOIN olist_silver.products p ON p.product_id = i.product_id
GROUP BY 1
ORDER BY revenue DESC
LIMIT 10;

-- 4. Late delivery rate by customer state
SELECT c.customer_state,
       count(*)                                                               AS delivered_orders,
       round(100.0 * sum(CASE WHEN o.is_late THEN 1 ELSE 0 END) / count(*), 2) AS late_pct,
       round(avg(o.delivery_days), 1)                                          AS avg_delivery_days
FROM olist_silver.orders o
JOIN olist_silver.customers c ON c.customer_id = o.customer_id
WHERE o.is_late IS NOT NULL
GROUP BY 1
HAVING count(*) >= 100
ORDER BY late_pct DESC;

-- 5. Does lateness hurt review scores?
SELECT o.is_late,
       count(*)                      AS reviews,
       round(avg(r.review_score), 3) AS avg_score
FROM olist_silver.reviews r
JOIN olist_silver.orders o ON o.order_id = r.order_id
WHERE o.is_late IS NOT NULL
GROUP BY 1;

-- 6. Payment mix
SELECT payment_type,
       count(*)                            AS payments,
       round(sum(payment_value), 2)        AS total_value,
       round(avg(payment_installments), 2) AS avg_installments
FROM olist_silver.payments
GROUP BY 1
ORDER BY total_value DESC;

-- 7. The data quality flags, visible in SQL (should match dq_<batch>.json)
SELECT sum(CASE WHEN dq_delivered_without_date THEN 1 ELSE 0 END)        AS delivered_without_date,
       sum(CASE WHEN dq_date_without_delivered_status THEN 1 ELSE 0 END) AS date_without_delivered
FROM olist_silver.orders;