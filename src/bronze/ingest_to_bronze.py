import logging
import sys
import time
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_project_root))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, input_file_name, lit, md5
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from config import BRONZE_DATA_DIR, RAW_DATA_DIR, SOURCE_CONFIG

log = logging.getLogger(__name__)

SCHEMAS: dict[str, StructType] = {
    "customers": StructType(
        [
            StructField("customer_id", StringType(), False),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip_code", StringType(), True),
            StructField("country", StringType(), True),
            StructField("registration_date", StringType(), True),
            StructField("loyalty_tier", StringType(), True),
            StructField("birth_date", StringType(), True),
        ]
    ),
    "products": StructType(
        [
            StructField("product_id", StringType(), False),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("sub_category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("cost", DoubleType(), True),
            StructField("supplier", StringType(), True),
            StructField("stock_quantity", IntegerType(), True),
            StructField("added_date", StringType(), True),
        ]
    ),
    "orders": StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("order_status", StringType(), True),
            StructField("shipping_address", StringType(), True),
        ]
    ),
    "payments": StructType(
        [
            StructField("payment_id", StringType(), False),
            StructField("order_id", StringType(), True),
            StructField("payment_date", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("payment_method", StringType(), True),
            StructField("payment_status", StringType(), True),
            StructField("transaction_id", StringType(), True),
        ]
    ),
    "deliveries": StructType(
        [
            StructField("delivery_id", StringType(), False),
            StructField("order_id", StringType(), True),
            StructField("delivery_date", StringType(), True),
            StructField("carrier", StringType(), True),
            StructField("tracking_number", StringType(), True),
            StructField("delivery_status", StringType(), True),
            StructField("estimated_days", IntegerType(), True),
            StructField("actual_days", IntegerType(), True),
        ]
    ),
    "web_events": StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("visitor_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("page_url", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("device_type", StringType(), True),
            StructField("browser", StringType(), True),
        ]
    ),
}


def read_source(spark: SparkSession, table_name: str) -> DataFrame:
    cfg = SOURCE_CONFIG[table_name]
    source_path = str(RAW_DATA_DIR / cfg["file"])
    schema = SCHEMAS[table_name]

    log.info("Reading %s from %s", table_name, source_path)

    if cfg["format"] == "csv":
        df = spark.read.schema(schema).option("header", True).csv(source_path)
    elif cfg["format"] == "json":
        df = spark.read.schema(schema).json(source_path)
    else:
        raise ValueError(f"Unsupported format: {cfg['format']}")

    return df


def add_audit_columns(df: DataFrame, table_name: str) -> DataFrame:
    return (
        df.withColumn("_ingestion_ts", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_row_hash", md5(concat_ws("||", *df.columns)))
        .withColumn("_table_name", lit(table_name))
    )


def ingest_table(spark: SparkSession, table_name: str) -> dict:
    t0 = time.time()
    df = read_source(spark, table_name)
    raw_count = df.count()

    df_with_audit = add_audit_columns(df, table_name)

    output_path = str(BRONZE_DATA_DIR / table_name)
    log.info("Writing bronze.%s to %s (%s rows)", table_name, output_path, raw_count)

    df_with_audit.write.mode("append").format("delta").option(
        "mergeSchema", "true"
    ).save(output_path)

    elapsed = time.time() - t0

    return {
        "table": table_name,
        "raw_count": raw_count,
        "output_path": output_path,
        "elapsed_seconds": round(elapsed, 2),
    }


def ingest_all(spark: SparkSession, table_names: list[str] | None = None) -> list[dict]:
    if table_names is None:
        table_names = list(SOURCE_CONFIG.keys())

    results = []
    for name in table_names:
        try:
            result = ingest_table(spark, name)
            log.info(
                "  ✓ bronze.%s: %s rows in %.1fs",
                result["table"],
                result["raw_count"],
                result["elapsed_seconds"],
            )
            results.append(result)
        except Exception as e:
            log.error("  ✗ bronze.%s failed: %s", name, e)
            raise

    return results


def run_bronze(spark: SparkSession | None = None, table_names: list[str] | None = None) -> list[dict]:
    if spark is None:
        from spark_session import get_spark_session

        spark = get_spark_session("BronzeIngestion")

    BRONZE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    return ingest_all(spark, table_names)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    results = run_bronze()
    total_rows = sum(r["raw_count"] for r in results)
    total_time = sum(r["elapsed_seconds"] for r in results)
    log.info("Bronze layer complete: %s rows ingested in %.1fs", total_rows, total_time)
