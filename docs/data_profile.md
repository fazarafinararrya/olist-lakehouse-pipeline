# Olist dataset profile

Measured on the files in `data/source/` (pandas, all columns read as text). These numbers are the
reference the pipeline's reconciliation and data quality checks are designed around.

## Volumes

| table | rows | notes |
|---|---:|---|
| customers | 99,441 | 96,096 distinct `customer_unique_id` (a person can have several `customer_id`s) |
| geolocation | 1,000,163 | 261,831 fully duplicated rows; 19,015 zip prefixes; up to 1,146 points per prefix |
| order_items | 112,650 | |
| payments | 103,886 | |
| reviews | 99,224 | multi-line comments (3,852 messages contain line breaks) |
| orders | 99,441 | purchases from 2016-09-04 to 2018-10-17 |
| products | 32,951 | |
| sellers | 3,095 | |
| category_translation | 71 | file starts with a UTF-8 BOM |

## Known data issues and how the pipeline treats them

| issue | count | treatment |
|---|---:|---|
| `review_id` is not unique (same review attached to several orders) | 814 repeated ids | natural key is `(review_id, order_id)`, which is unique; all rows kept |
| orders with more than one review | 547 | kept; gold must pick one (latest) per order |
| review comments contain line breaks and quotes | 3,852 | raw read uses `multiLine`, `quote`, `escape`; silver collapses whitespace |
| products without category (and without name/description/photo metadata) | 610 | category = `unknown`, `category_translation_source = missing` |
| categories absent from the official translation | 2 (`pc_gamer`, `portateis_cozinha_e_preparadores_de_alimentos`) | documented manual override, source = `manual_override` |
| source column typos `product_name_lenght`, `product_description_lenght` | 2 columns | kept in raw (contract), renamed in silver |
| `delivered` orders without a delivery date | 8 | flag `dq_delivered_without_date` (warn) |
| non-delivered orders that have a delivery date | 6 | flag `dq_date_without_delivered_status` (warn) |
| orders never approved | 160 (141 canceled, 14 delivered, 5 created) | kept; null `order_approved_at` |
| payments with value 0 | 9 | flag `dq_zero_value` (warn) |
| payments with 0 installments | 2 | flag `dq_zero_installments` (warn) |
| payment type `not_defined` | 3 | flag `dq_type_not_defined` (warn) |
| orders with no items / no payments / no reviews | 775 / 1 / 768 | kept; matters for gold joins (use left joins) |
| geolocation exact duplicates | 261,831 | dropped in silver |
| geolocation points outside Brazil's bounding box | 33 after de-duplication | moved to `geolocation_rejects` with a reason |
| customer / seller zip prefixes missing from geolocation | ~157-158 customer, 7 seller zip prefixes | warn only (`fk` checks with severity warn) |
| referential integrity items->orders/products/sellers, orders->customers, payments/reviews->orders | 0 orphans | enforced as errors |

## Expected silver row counts (pandas reference implementation)

If a run disagrees with these, investigate before trusting it.

| silver table | expected rows |
|---|---:|
| orders | 99,441 |
| customers | 99,441 |
| sellers | 3,095 |
| order_items | 112,650 |
| payments | 103,886 |
| reviews | 99,224 |
| products | 32,951 |
| geolocation (valid points) | ~720,463 |
| geolocation_rejects | ~33 |
| geolocation_zip | ~19,010 (5 prefixes have only invalid points) |

The geolocation figures come from a pandas re-implementation of the same normalisation
(trim, lower-case, accent removal, upper-case state, de-duplicate, bounding box). Floating-point
parsing can differ in the last digits, so treat them as "expected, within a handful of rows".
