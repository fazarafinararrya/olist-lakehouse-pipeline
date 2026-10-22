from decimal import Decimal

from olist_pipeline import silver


def by(df, key):
    return {r[key]: r.asDict() for r in df.collect()}


# ------------------------------------------------------------------ orders
def test_orders_delivery_metrics_and_flags(make_raw):
    raw = make_raw(
        "orders",
        [
            # delivered 2 days after the estimate, 9 days after purchase
            dict(order_id="o1", customer_id="c1", order_status="delivered",
                 order_purchase_timestamp="2018-01-01 10:00:00",
                 order_delivered_customer_date="2018-01-10 10:00:00",
                 order_estimated_delivery_date="2018-01-08 00:00:00"),
            # status says delivered but there is no delivery date (known upstream issue)
            dict(order_id="o2", customer_id="c2", order_status="delivered",
                 order_purchase_timestamp="2018-01-01 10:00:00",
                 order_estimated_delivery_date="2018-01-08 00:00:00"),
            # not delivered yet but already has a delivery date
            dict(order_id="o3", customer_id="c3", order_status=" Shipped ",
                 order_purchase_timestamp="2018-01-01 10:00:00",
                 order_delivered_customer_date="2018-01-05 10:00:00",
                 order_estimated_delivery_date="2018-01-08 00:00:00"),
        ],
    )
    rows = by(silver.clean_orders(raw), "order_id")

    assert rows["o1"]["delivery_days"] == 9
    assert rows["o1"]["delivery_delay_days"] == 2
    assert rows["o1"]["is_late"] is True
    assert rows["o1"]["dq_delivered_without_date"] is False

    assert rows["o2"]["dq_delivered_without_date"] is True
    assert rows["o2"]["is_late"] is None  # cannot be judged without a delivery date

    assert rows["o3"]["order_status"] == "shipped"  # trimmed + lower-cased
    assert rows["o3"]["dq_date_without_delivered_status"] is True
    assert rows["o3"]["is_delivered"] is False


def test_orders_row_count_is_preserved(make_raw):
    raw = make_raw("orders", [dict(order_id=f"o{i}", customer_id="c", order_status="created") for i in range(5)])
    assert silver.clean_orders(raw).count() == 5


# ------------------------------------------------------------------ customers / sellers
def test_customers_zip_padding_city_accents_and_state(make_raw):
    raw = make_raw(
        "customers",
        [dict(customer_id="c1", customer_unique_id="u1", customer_zip_code_prefix="1037",
              customer_city="  São Paulo ", customer_state="sp")],
    )
    row = silver.clean_customers(raw).collect()[0]
    assert row["customer_zip_code_prefix"] == "01037"
    assert row["customer_city"] == "sao paulo"
    assert row["customer_state"] == "SP"


# ------------------------------------------------------------------ order items / payments
def test_order_items_types_and_total(make_raw):
    raw = make_raw(
        "order_items",
        [dict(order_id="o1", order_item_id="1", product_id="p1", seller_id="s1",
              shipping_limit_date="2017-09-19 09:45:35", price="58.90", freight_value="13.29")],
    )
    row = silver.clean_order_items(raw).collect()[0]
    assert row["order_item_id"] == 1
    assert row["price"] == Decimal("58.90")
    assert row["item_total"] == Decimal("72.19")


def test_payments_flags(make_raw):
    raw = make_raw(
        "payments",
        [
            dict(order_id="o1", payment_sequential="1", payment_type="credit_card",
                 payment_installments="3", payment_value="100.00"),
            dict(order_id="o2", payment_sequential="1", payment_type="not_defined",
                 payment_installments="0", payment_value="0.00"),
        ],
    )
    rows = by(silver.clean_payments(raw), "order_id")
    assert rows["o1"]["dq_zero_value"] is False
    assert rows["o2"]["dq_zero_value"] is True
    assert rows["o2"]["dq_zero_installments"] is True
    assert rows["o2"]["dq_type_not_defined"] is True


# ------------------------------------------------------------------ reviews
def test_reviews_multiline_blank_and_composite_key(make_raw):
    raw = make_raw(
        "reviews",
        [
            dict(review_id="r1", order_id="o1", review_score="5", review_comment_title="  ",
                 review_comment_message="great\nproduct,   fast  delivery",
                 review_creation_date="2018-01-18 00:00:00",
                 review_answer_timestamp="2018-01-18 21:46:59"),
            # same review_id on another order is legitimate upstream -> must be kept
            dict(review_id="r1", order_id="o2", review_score="5"),
            # exact duplicate of the first row -> removed
            dict(review_id="r1", order_id="o1", review_score="5", review_comment_title="  ",
                 review_comment_message="great\nproduct,   fast  delivery",
                 review_creation_date="2018-01-18 00:00:00",
                 review_answer_timestamp="2018-01-18 21:46:59"),
        ],
    )
    df = silver.clean_reviews(raw)
    assert df.count() == 2
    first = by(df, "order_id")["o1"]
    assert first["review_comment_message"] == "great product, fast delivery"
    assert first["review_comment_title"] is None
    assert first["has_comment"] is True
    assert by(df, "order_id")["o2"]["has_comment"] is False


# ------------------------------------------------------------------ products
def test_products_translation_overrides_and_missing(make_raw):
    translation = make_raw(
        "category_translation",
        [dict(product_category_name="beleza_saude", product_category_name_english="health_beauty")],
    )
    products = make_raw(
        "products",
        [
            dict(product_id="p1", product_category_name="beleza_saude", product_name_lenght="40"),
            dict(product_id="p2", product_category_name="pc_gamer"),
            dict(product_id="p3", product_category_name=None),
            dict(product_id="p4", product_category_name="categoria_nova"),
        ],
    )
    rows = by(silver.clean_products(products, translation), "product_id")

    assert rows["p1"]["product_category_name_english"] == "health_beauty"
    assert rows["p1"]["category_translation_source"] == "official"
    assert rows["p1"]["product_name_length"] == 40  # typo'd source column fixed

    assert rows["p2"]["category_translation_source"] == "manual_override"
    assert rows["p3"]["product_category_name_english"] == "unknown"
    assert rows["p3"]["category_translation_source"] == "missing"
    assert rows["p4"]["category_translation_source"] == "untranslated"
    assert rows["p4"]["product_category_name_english"] == "categoria_nova"


def test_products_join_does_not_duplicate_rows(make_raw):
    translation = make_raw(
        "category_translation",
        [dict(product_category_name="pc_gamer", product_category_name_english="pc_gamer")],
    )
    products = make_raw("products", [dict(product_id="p1", product_category_name="pc_gamer")])
    # the override must not add a second mapping when the official file already has the key
    assert silver.clean_products(products, translation).count() == 1


# ------------------------------------------------------------------ geolocation
def _geo(zip_, lat, lng, city="sao paulo", state="SP"):
    return dict(geolocation_zip_code_prefix=zip_, geolocation_lat=str(lat),
                geolocation_lng=str(lng), geolocation_city=city, geolocation_state=state)


def test_geolocation_dedup_and_bounds(make_raw):
    raw = make_raw(
        "geolocation",
        [
            _geo("01037", -23.5, -46.6),
            _geo("01037", -23.5, -46.6),          # exact duplicate
            _geo("01037", -23.7, -46.8),
            _geo("99999", 40.0, -3.0, "madrid"),   # outside Brazil
            _geo("88888", "abc", -46.0),           # not a number
        ],
    )
    valid, rejects = silver.clean_geolocation(raw)
    assert valid.count() == 2
    assert rejects.count() == 2
    assert {r["reject_reason"] for r in rejects.collect()} == {"coordinates_null_or_outside_brazil"}


def test_geolocation_zip_is_one_row_per_prefix(make_raw):
    raw = make_raw(
        "geolocation",
        [
            _geo("01037", -23.0, -46.0, "sao paulo"),
            _geo("01037", -24.0, -47.0, "sao paulo"),
            _geo("01037", -25.0, -48.0, "osasco"),
            _geo("20000", -22.9, -43.2, "rio de janeiro", "RJ"),
        ],
    )
    valid, _ = silver.clean_geolocation(raw)
    zips = by(silver.build_geolocation_zip(valid), "zip_code_prefix")

    assert set(zips) == {"01037", "20000"}
    assert zips["01037"]["n_points"] == 3
    assert zips["01037"]["city"] == "sao paulo"      # most frequent
    assert round(zips["01037"]["latitude"], 6) == -24.0  # mean of -23, -24, -25
    assert zips["20000"]["state"] == "RJ"
