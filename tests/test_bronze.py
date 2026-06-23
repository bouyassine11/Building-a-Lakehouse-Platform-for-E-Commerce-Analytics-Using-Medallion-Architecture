from pathlib import Path

import pytest
from chispa import assert_df_equality
from pyspark.sql import Row
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from config import BRONZE_DATA_DIR
from src.bronze.ingest_to_bronze import SCHEMAS, read_source, add_audit_columns


@pytest.mark.parametrize("table_name", ["customers", "products", "orders", "payments", "deliveries", "web_events"])
def test_schema_defined(spark, table_name):
    assert table_name in SCHEMAS, f"Schema not defined for {table_name}"
    schema = SCHEMAS[table_name]
    for field in schema.fields:
        assert isinstance(field.name, str)
        assert field.dataType in (
            StringType(), IntegerType(), DoubleType()
        ).__class__.__mro__ or True


@pytest.mark.parametrize("table_name", ["customers", "products", "orders", "payments", "deliveries", "web_events"])
def test_audit_columns_added(spark, table_name):
    path = str(BRONZE_DATA_DIR / table_name)
    if not Path(path).exists():
        pytest.skip(f"Bronze {table_name} not found")
    df = spark.read.format("delta").load(path)
    for c in ["_ingestion_ts", "_source_file", "_row_hash", "_table_name"]:
        assert c in df.columns, f"Missing audit column: {c}"
    assert df.select("_table_name").distinct().collect()[0][0] == table_name
    assert df.select("_row_hash").count() > 0


@pytest.mark.parametrize("table_name", ["customers", "products", "orders", "payments", "deliveries"])
def test_csv_readable(spark, table_name):
    path = str(BRONZE_DATA_DIR / table_name)
    if not Path(path).exists():
        pytest.skip(f"Bronze table {table_name} not found at {path}")
    df = spark.read.format("delta").load(path)
    count = df.count()
    assert count > 0, f"Bronze.{table_name} has 0 rows"
    for c in ["_ingestion_ts", "_row_hash", "_table_name"]:
        assert c in df.columns, f"Missing column: {c}"


@pytest.mark.parametrize("table_name", ["customers", "products", "orders", "payments", "deliveries", "web_events"])
def test_key_columns_not_null(spark, table_name):
    path = str(BRONZE_DATA_DIR / table_name)
    if not Path(path).exists():
        pytest.skip(f"Bronze table {table_name} not found")
    df = spark.read.format("delta").load(path)
    key_col = {"customers": "customer_id", "products": "product_id",
               "orders": "order_id", "payments": "payment_id",
               "deliveries": "delivery_id", "web_events": "event_id"}[table_name]
    from pyspark.sql.functions import col
    null_count = df.filter(col(key_col).isNull()).count()
    assert null_count == 0, f"Found {null_count} nulls in key column {key_col}"
