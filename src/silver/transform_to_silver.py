import logging
import sys
import time
from pathlib import Path
from typing import Any

_project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_project_root))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, col, lit, row_number, to_date, when
from pyspark.sql.window import Window

from config import BRONZE_DATA_DIR, SILVER_DATA_DIR

log = logging.getLogger(__name__)

DATE_PATTERNS = ["yyyy-MM-dd", "MM/dd/yyyy", "dd-MM-yyyy", "yyyyMMdd"]


def parse_date_multi(df: DataFrame, col_name: str, target_name: str | None = None) -> DataFrame:
    target = target_name or col_name
    parsed = coalesce(*[to_date(col(col_name), p) for p in DATE_PATTERNS])
    return df.withColumn(target, parsed)


def dedup_by_key(df: DataFrame, key: str, order_col: str = "_ingestion_ts") -> DataFrame:
    before = df.count()
    w = Window.partitionBy(key).orderBy(col(order_col).desc())
    deduped = df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")
    after = deduped.count()
    if before - after:
        log.info("  Dedup %s on %s: %s -> %s (removed %s)", key, key, before, after, before - after)
    return deduped


def fill_nulls(df: DataFrame, fills: dict[str, Any]) -> DataFrame:
    result = df
    for col_name, default in fills.items():
        result = result.fillna({col_name: default})
    return result


def add_silver_metadata(df: DataFrame) -> DataFrame:
    return df.withColumn("_silver_ingested_at", lit(time.strftime("%Y-%m-%d %H:%M:%S")))


def validate_fk(df: DataFrame, ref_path: str, fk_col: str, ref_col: str, spark: SparkSession) -> DataFrame:
    if not Path(ref_path).exists():
        return df

    ref_df = spark.read.format("delta").load(ref_path).select(ref_col).distinct()
    ref_df = ref_df.withColumnRenamed(ref_col, "_fk_valid_id")
    before = df.count()
    df_valid = df.join(ref_df.hint("broadcast"), col(fk_col) == col("_fk_valid_id"), "inner").drop("_fk_valid_id")
    after = df_valid.count()
    if before - after:
        log.info("  FK %s -> %s: removed %s rows", fk_col, ref_col, before - after)
    return df_valid


def transform_customers(bronze_path: str, silver_path: str, spark: SparkSession) -> dict:
    log.info("Transforming customers...")
    df = spark.read.format("delta").load(bronze_path)
    initial = df.count()

    df = dedup_by_key(df, "customer_id")
    df = dedup_by_key(df, "email")
    df = parse_date_multi(df, "registration_date", "registration_date")
    df = parse_date_multi(df, "birth_date", "birth_date")
    df = fill_nulls(df, {
        "email": "unknown@email.com",
        "first_name": "Unknown",
        "last_name": "Unknown",
        "phone": "Unknown",
        "address": "Unknown",
        "city": "Unknown",
        "state": "Unknown",
        "zip_code": "00000",
        "country": "Unknown",
        "loyalty_tier": "Bronze",
    })
    df = df.drop("_source_file", "_row_hash", "_table_name", "_ingestion_ts")
    df = add_silver_metadata(df)
    df.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(silver_path)
    final = df.count()
    return {"table": "customers", "initial": initial, "final": final, "rejected": initial - final}


def transform_products(bronze_path: str, silver_path: str, spark: SparkSession) -> dict:
    log.info("Transforming products...")
    df = spark.read.format("delta").load(bronze_path)
    initial = df.count()

    df = dedup_by_key(df, "product_id")
    df = parse_date_multi(df, "added_date", "added_date")
    df = fill_nulls(df, {
        "category": "Miscellaneous",
        "sub_category": "General",
        "product_name": "Unknown Product",
        "supplier": "Unknown",
    })
    df = df.withColumn("price", when(col("price") <= 0, lit(0.01)).otherwise(col("price")))
    df = df.withColumn("cost", when(col("cost") <= 0, lit(0.01)).otherwise(col("cost")))
    df = df.withColumn("stock_quantity", when(col("stock_quantity") < 0, lit(0)).otherwise(col("stock_quantity")))
    df = df.drop("_source_file", "_row_hash", "_table_name", "_ingestion_ts")
    df = add_silver_metadata(df)
    df.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(silver_path)
    final = df.count()
    return {"table": "products", "initial": initial, "final": final, "rejected": initial - final}


def transform_orders(bronze_path: str, silver_path: str, spark: SparkSession) -> dict:
    log.info("Transforming orders...")
    df = spark.read.format("delta").load(bronze_path)
    initial = df.count()

    df = dedup_by_key(df, "order_id")
    df = parse_date_multi(df, "order_date", "order_date")
    df = fill_nulls(df, {
        "order_status": "Unknown",
        "shipping_address": "Unknown",
    })
    df = df.withColumn("quantity", when(col("quantity") <= 0, lit(1)).otherwise(col("quantity")))
    df = df.withColumn("total_amount", col("quantity") * col("unit_price"))

    # Validate FK against Bronze (source of truth for all IDs)
    # Silver dimensions may drop near-dupes, but the original IDs are valid
    df = validate_fk(df, str(BRONZE_DATA_DIR / "customers"), "customer_id", "customer_id", spark)
    df = validate_fk(df, str(BRONZE_DATA_DIR / "products"), "product_id", "product_id", spark)

    df = df.drop("_source_file", "_row_hash", "_table_name", "_ingestion_ts")
    df = add_silver_metadata(df)
    df.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(silver_path)
    final = df.count()
    return {"table": "orders", "initial": initial, "final": final}


def transform_payments(bronze_path: str, silver_path: str, spark: SparkSession) -> dict:
    log.info("Transforming payments...")
    df = spark.read.format("delta").load(bronze_path)
    initial = df.count()

    df = dedup_by_key(df, "payment_id")
    df = parse_date_multi(df, "payment_date", "payment_date")
    df = fill_nulls(df, {"amount": 0.0, "payment_method": "Unknown"})
    df = validate_fk(df, str(BRONZE_DATA_DIR / "orders"), "order_id", "order_id", spark)
    df = df.drop("_source_file", "_row_hash", "_table_name", "_ingestion_ts")
    df = add_silver_metadata(df)
    df.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(silver_path)
    final = df.count()
    return {"table": "payments", "initial": initial, "final": final}


def transform_deliveries(bronze_path: str, silver_path: str, spark: SparkSession) -> dict:
    log.info("Transforming deliveries...")
    df = spark.read.format("delta").load(bronze_path)
    initial = df.count()

    df = dedup_by_key(df, "delivery_id")
    df = parse_date_multi(df, "delivery_date", "delivery_date")
    df = fill_nulls(df, {
        "carrier": "Unknown",
        "tracking_number": "Unknown",
        "delivery_status": "Unknown",
    })
    df = validate_fk(df, str(BRONZE_DATA_DIR / "orders"), "order_id", "order_id", spark)
    df = df.drop("_source_file", "_row_hash", "_table_name", "_ingestion_ts")
    df = add_silver_metadata(df)
    df.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(silver_path)
    final = df.count()
    return {"table": "deliveries", "initial": initial, "final": final}


def transform_web_events(bronze_path: str, silver_path: str, spark: SparkSession) -> dict:
    log.info("Transforming web_events...")
    df = spark.read.format("delta").load(bronze_path)
    initial = df.count()

    df = dedup_by_key(df, "event_id")
    df = fill_nulls(df, {
        "product_id": "N/A",
        "browser": "Unknown",
        "device_type": "Unknown",
    })
    df = df.drop("_source_file", "_row_hash", "_table_name", "_ingestion_ts")
    df = add_silver_metadata(df)
    df.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(silver_path)
    final = df.count()
    return {"table": "web_events", "initial": initial, "final": final, "rejected": initial - final}


def run_silver(spark: SparkSession | None = None) -> list[dict]:
    if spark is None:
        from spark_session import get_spark_session
        spark = get_spark_session("SilverTransformation")

    SILVER_DATA_DIR.mkdir(parents=True, exist_ok=True)
    results = []

    for name in ["customers", "products", "web_events"]:
        t0 = time.time()
        func = globals()[f"transform_{name}"]
        result = func(
            bronze_path=str(BRONZE_DATA_DIR / name),
            silver_path=str(SILVER_DATA_DIR / name),
            spark=spark,
        )
        result["elapsed"] = round(time.time() - t0, 1)
        results.append(result)
        log.info("  ✓ silver.%s: %s -> %s rows in %.1fs", result["table"], result["initial"], result["final"], result["elapsed"])

    t0 = time.time()
    result = transform_orders(
        bronze_path=str(BRONZE_DATA_DIR / "orders"),
        silver_path=str(SILVER_DATA_DIR / "orders"),
        spark=spark,
    )
    result["elapsed"] = round(time.time() - t0, 1)
    results.append(result)
    log.info("  ✓ silver.orders: %s -> %s rows in %.1fs", result["initial"], result["final"], result["elapsed"])

    for name in ["payments", "deliveries"]:
        t0 = time.time()
        func = globals()[f"transform_{name}"]
        result = func(
            bronze_path=str(BRONZE_DATA_DIR / name),
            silver_path=str(SILVER_DATA_DIR / name),
            spark=spark,
        )
        result["elapsed"] = round(time.time() - t0, 1)
        results.append(result)
        log.info("  ✓ silver.%s: %s -> %s rows in %.1fs", result["table"], result["initial"], result["final"], result["elapsed"])

    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    t0 = time.time()
    results = run_silver()
    total_initial = sum(r["initial"] for r in results)
    total_final = sum(r["final"] for r in results)
    log.info("Silver layer complete: %s -> %s rows in %.1fs", total_initial, total_final, time.time() - t0)
