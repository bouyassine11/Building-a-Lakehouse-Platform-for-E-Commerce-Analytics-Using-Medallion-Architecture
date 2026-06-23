from pathlib import Path

import pytest
from pyspark.sql.functions import col, count, when

from config import SILVER_DATA_DIR, BRONZE_DATA_DIR


@pytest.mark.parametrize("table_name", ["customers", "products", "orders", "payments", "deliveries", "web_events"])
def test_silver_exists(spark, table_name):
    path = SILVER_DATA_DIR / table_name
    assert Path(path).exists(), f"Silver table {table_name} does not exist"
    df = spark.read.format("delta").load(str(path))
    assert df.count() > 0, f"Silver table {table_name} is empty"


def test_customers_no_duplicate_ids(spark):
    path = str(SILVER_DATA_DIR / "customers")
    if not Path(path).exists():
        pytest.skip("Silver customers not found")
    df = spark.read.format("delta").load(path)
    total = df.count()
    distinct = df.select("customer_id").distinct().count()
    assert total == distinct, f"Customers has {total - distinct} duplicate customer_ids"


def test_customers_no_null_emails(spark):
    path = str(SILVER_DATA_DIR / "customers")
    if not Path(path).exists():
        pytest.skip("Silver customers not found")
    df = spark.read.format("delta").load(path)
    nulls = df.filter(col("email").isNull()).count()
    assert nulls == 0, f"Found {nulls} customers with null email"


def test_customers_all_dates_parsed(spark):
    path = str(SILVER_DATA_DIR / "customers")
    if not Path(path).exists():
        pytest.skip("Silver customers not found")
    df = spark.read.format("delta").load(path)
    null_reg = df.filter(col("registration_date").isNull()).count()
    null_birth = df.filter(col("birth_date").isNull()).count()
    assert null_reg == 0, f"{null_reg} customers with null registration_date"
    assert null_birth == 0, f"{null_birth} customers with null birth_date"


def test_products_no_negative_prices(spark):
    path = str(SILVER_DATA_DIR / "products")
    if not Path(path).exists():
        pytest.skip("Silver products not found")
    df = spark.read.format("delta").load(path)
    neg_prices = df.filter(col("price") <= 0).count()
    assert neg_prices == 0, f"Found {neg_prices} products with price <= 0"


def test_products_no_null_categories(spark):
    path = str(SILVER_DATA_DIR / "products")
    if not Path(path).exists():
        pytest.skip("Silver products not found")
    df = spark.read.format("delta").load(path)
    null_cats = df.filter(col("category").isNull()).count()
    assert null_cats == 0, f"Found {null_cats} products with null category"


def test_orders_no_negative_quantity(spark):
    path = str(SILVER_DATA_DIR / "orders")
    if not Path(path).exists():
        pytest.skip("Silver orders not found")
    df = spark.read.format("delta").load(path)
    neg = df.filter(col("quantity") <= 0).count()
    assert neg == 0, f"Found {neg} orders with quantity <= 0"


def test_orders_all_dates_parsed(spark):
    path = str(SILVER_DATA_DIR / "orders")
    if not Path(path).exists():
        pytest.skip("Silver orders not found")
    df = spark.read.format("delta").load(path)
    null_dates = df.filter(col("order_date").isNull()).count()
    assert null_dates == 0, f"Found {null_dates} orders with null order_date"


def test_fk_orders_reference_valid_customers(spark):
    orders_path = str(SILVER_DATA_DIR / "orders")
    cust_path = str(BRONZE_DATA_DIR / "customers")
    if not Path(orders_path).exists() or not Path(cust_path).exists():
        pytest.skip("Silver orders or Bronze customers not found")
    orders = spark.read.format("delta").load(orders_path)
    customers = spark.read.format("delta").load(cust_path).select("customer_id").distinct()
    valid_ids = {r.customer_id for r in customers.collect()}
    invalid = orders.filter(~col("customer_id").isin(valid_ids)).count()
    assert invalid == 0, f"Found {invalid} orders with invalid customer_id"


def test_fk_payments_reference_valid_orders(spark):
    pay_path = str(SILVER_DATA_DIR / "payments")
    ord_path = str(BRONZE_DATA_DIR / "orders")
    if not Path(pay_path).exists() or not Path(ord_path).exists():
        pytest.skip("Silver payments or Bronze orders not found")
    payments = spark.read.format("delta").load(pay_path)
    orders = spark.read.format("delta").load(ord_path).select("order_id").distinct()
    valid_ids = {r.order_id for r in orders.collect()}
    invalid = payments.filter(~col("order_id").isin(valid_ids)).count()
    assert invalid == 0, f"Found {invalid} payments with invalid order_id"


def test_payments_no_null_amounts(spark):
    path = str(SILVER_DATA_DIR / "payments")
    if not Path(path).exists():
        pytest.skip("Silver payments not found")
    df = spark.read.format("delta").load(path)
    nulls = df.filter(col("amount").isNull()).count()
    assert nulls == 0, f"Found {nulls} payments with null amount"
