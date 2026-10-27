-- Schema of the fictional operational system that Airbyte will read from.
-- Types are realistic (timestamps, numerics, primary keys) rather than all-text:
-- the point of this layer is to look like a real source database.

DROP SCHEMA IF EXISTS olist CASCADE;
CREATE SCHEMA olist;

CREATE TABLE olist.customers (
    customer_id              text PRIMARY KEY,
    customer_unique_id       text NOT NULL,
    customer_zip_code_prefix text NOT NULL,   -- text: leading zeros are significant
    customer_city            text,
    customer_state           text
);

CREATE TABLE olist.sellers (
    seller_id              text PRIMARY KEY,
    seller_zip_code_prefix text NOT NULL,
    seller_city            text,
    seller_state           text
);

CREATE TABLE olist.geolocation (
    geolocation_zip_code_prefix text,
    geolocation_lat             double precision,
    geolocation_lng             double precision,
    geolocation_city            text,
    geolocation_state           text
);  -- no primary key upstream: many points per zip prefix, with duplicates

CREATE TABLE olist.products (
    product_id                 text PRIMARY KEY,
    product_category_name      text,
    product_name_lenght        integer,       -- source misspelling kept on purpose
    product_description_lenght integer,
    product_photos_qty         integer,
    product_weight_g           integer,
    product_length_cm          integer,
    product_height_cm          integer,
    product_width_cm           integer
);

CREATE TABLE olist.product_category_name_translation (
    product_category_name         text PRIMARY KEY,
    product_category_name_english text NOT NULL
);

CREATE TABLE olist.orders (
    order_id                      text PRIMARY KEY,
    customer_id                   text NOT NULL REFERENCES olist.customers (customer_id),
    order_status                  text NOT NULL,
    order_purchase_timestamp      timestamp NOT NULL,
    order_approved_at             timestamp,
    order_delivered_carrier_date  timestamp,
    order_delivered_customer_date timestamp,
    order_estimated_delivery_date timestamp
);

CREATE TABLE olist.order_items (
    order_id            text NOT NULL REFERENCES olist.orders (order_id),
    order_item_id       integer NOT NULL,
    product_id          text NOT NULL REFERENCES olist.products (product_id),
    seller_id           text NOT NULL REFERENCES olist.sellers (seller_id),
    shipping_limit_date timestamp,
    price               numeric(12,2) NOT NULL,
    freight_value       numeric(12,2) NOT NULL,
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE olist.order_payments (
    order_id             text NOT NULL REFERENCES olist.orders (order_id),
    payment_sequential   integer NOT NULL,
    payment_type         text,
    payment_installments integer,
    payment_value        numeric(12,2),
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE olist.order_reviews (
    review_id               text NOT NULL,
    order_id                text NOT NULL REFERENCES olist.orders (order_id),
    review_score            integer,
    review_comment_title    text,
    review_comment_message  text,
    review_creation_date    timestamp,
    review_answer_timestamp timestamp,
    -- review_id alone is not unique upstream; the natural key is the pair
    PRIMARY KEY (review_id, order_id)
);

-- Indexes an operational system would realistically have, and which also make
-- Airbyte's incremental reads cheaper.
CREATE INDEX idx_orders_purchase_ts ON olist.orders (order_purchase_timestamp);
CREATE INDEX idx_order_items_order  ON olist.order_items (order_id);
CREATE INDEX idx_reviews_created    ON olist.order_reviews (review_creation_date);