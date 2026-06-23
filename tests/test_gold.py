from pathlib import Path

import pytest
from pyspark.sql.functions import col

from config import GOLD_DATA_DIR


@pytest.mark.parametrize("table", ["dim_date", "dim_customer", "dim_product", "dim_location",
                                    "fact_order", "fact_payment", "fact_delivery"])
def test_gold_table_exists(spark, table):
    path = GOLD_DATA_DIR / table
    assert Path(path).exists(), f"Gold table {table} does not exist"
    df = spark.read.format("delta").load(str(path))
    assert df.count() > 0, f"Gold table {table} is empty"


def test_dim_customer_surrogate_keys_unique(spark):
    path = str(GOLD_DATA_DIR / "dim_customer")
    if not Path(path).exists():
        pytest.skip("dim_customer not found")
    df = spark.read.format("delta").load(path)
    total = df.count()
    distinct = df.select("customer_key").distinct().count()
    assert total == distinct, f"dim_customer has {total - distinct} duplicate customer_keys"


def test_dim_product_surrogate_keys_unique(spark):
    path = str(GOLD_DATA_DIR / "dim_product")
    if not Path(path).exists():
        pytest.skip("dim_product not found")
    df = spark.read.format("delta").load(path)
    total = df.count()
    distinct = df.select("product_key").distinct().count()
    assert total == distinct, f"dim_product has {total - distinct} duplicate product_keys"


def test_fact_order_fks_resolve(spark):
    fact_path = str(GOLD_DATA_DIR / "fact_order")
    dim_cust = str(GOLD_DATA_DIR / "dim_customer")
    dim_prod = str(GOLD_DATA_DIR / "dim_product")
    if not all(Path(p).exists() for p in [fact_path, dim_cust, dim_prod]):
        pytest.skip("Gold tables not found")
    fact = spark.read.format("delta").load(fact_path)
    cust = spark.read.format("delta").load(dim_cust).select("customer_key").distinct()
    prod = spark.read.format("delta").load(dim_prod).select("product_key").distinct()
    cust_keys = {r.customer_key for r in cust.collect()}
    prod_keys = {r.product_key for r in prod.collect()}
    invalid_cust = fact.filter(~col("customer_key").isin(cust_keys)).count()
    invalid_prod = fact.filter(~col("product_key").isin(prod_keys)).count()
    assert invalid_cust == 0, f"fact_order has {invalid_cust} invalid customer_keys"
    assert invalid_prod == 0, f"fact_order has {invalid_prod} invalid product_keys"


def test_dim_date_covers_range(spark):
    path = str(GOLD_DATA_DIR / "dim_date")
    if not Path(path).exists():
        pytest.skip("dim_date not found")
    df = spark.read.format("delta").load(path)
    years = [r.year for r in df.select("year").distinct().orderBy("year").collect()]
    assert 2020 in years, "dim_date missing year 2020"
    assert 2026 in years, "dim_date missing year 2026"
    assert len(years) >= 7, f"dim_date only has {len(years)} years, expected 7+"


def test_dim_location_distinct(spark):
    path = str(GOLD_DATA_DIR / "dim_location")
    if not Path(path).exists():
        pytest.skip("dim_location not found")
    df = spark.read.format("delta").load(path)
    total = df.count()
    distinct = df.select("city", "state", "zip_code", "country").distinct().count()
    assert total == distinct, f"dim_location has {total - distinct} duplicate locations"
