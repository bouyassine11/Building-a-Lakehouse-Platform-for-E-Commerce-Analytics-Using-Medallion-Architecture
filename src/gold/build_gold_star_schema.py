import logging
import sys
import time
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_project_root))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    date_add,
    dayofmonth,
    dayofweek,
    dayofyear,
    lit,
    lpad,
    monotonically_increasing_id,
    month,
    quarter,
    row_number,
    to_date,
    udf,
    weekofyear,
    when,
    year,
)
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from config import GOLD_DATA_DIR, SILVER_DATA_DIR

log = logging.getLogger(__name__)


def build_dim_date(spark: SparkSession, start: str = "2020-01-01", end: str = "2026-12-31") -> DataFrame:
    log.info("Building dim_date from %s to %s...", start, end)
    start_date = to_date(lit(start))
    end_date = to_date(lit(end))
    days = spark.sql(
        f"SELECT sequence(date'{start}', date'{end}', interval 1 day) as days"
    ).selectExpr("explode(days) as date_key")

    df = days.select(
        col("date_key"),
        year("date_key").alias("year"),
        month("date_key").alias("month"),
        dayofmonth("date_key").alias("day"),
        dayofweek("date_key").alias("day_of_week"),
        dayofyear("date_key").alias("day_of_year"),
        weekofyear("date_key").alias("week_of_year"),
        quarter("date_key").alias("quarter"),
        lpad(month("date_key"), 2, "0").alias("month_str"),
        when(dayofweek("date_key").isin(1, 7), lit(True)).otherwise(lit(False)).alias("is_weekend"),
        when(month("date_key") == 1, dayofmonth("date_key").isin(1)).otherwise(lit(False)).alias("is_holiday"),
    ).withColumn(
        "year_month", concat(col("year"), lit("-"), lpad(col("month"), 2, "0"))
    ).withColumn(
        "quarter_label", concat(lit("Q"), col("quarter"), lit(" "), col("year"))
    )

    return df


def build_dim_customer(spark: SparkSession) -> DataFrame:
    log.info("Building dim_customer...")
    df = spark.read.format("delta").load(str(SILVER_DATA_DIR / "customers"))

    df = df.withColumn(
        "customer_key",
        row_number().over(Window.orderBy("customer_id")).cast(IntegerType()),
    ).withColumn("valid_from", lit(time.strftime("%Y-%m-%d")))
    df = df.withColumn("valid_to", lit("9999-12-31")).withColumn("is_current", lit(True))

    cols = ["customer_key", "customer_id", "first_name", "last_name", "email",
            "phone", "address", "city", "state", "zip_code", "country",
            "registration_date", "loyalty_tier", "birth_date",
            "valid_from", "valid_to", "is_current"]
    return df.select(*[c for c in cols if c in df.columns])


def build_dim_product(spark: SparkSession) -> DataFrame:
    log.info("Building dim_product...")
    df = spark.read.format("delta").load(str(SILVER_DATA_DIR / "products"))

    df = df.withColumn(
        "product_key",
        row_number().over(Window.orderBy("product_id")).cast(IntegerType()),
    )

    cols = ["product_key", "product_id", "product_name", "category",
            "sub_category", "price", "cost", "supplier", "stock_quantity",
            "added_date"]
    return df.select(*[c for c in cols if c in df.columns])


def build_dim_location(spark: SparkSession) -> DataFrame:
    log.info("Building dim_location...")
    df = spark.read.format("delta").load(str(SILVER_DATA_DIR / "customers"))

    df = df.select("city", "state", "zip_code", "country").distinct()
    df = df.withColumn(
        "location_key",
        row_number().over(Window.orderBy("city", "state", "zip_code", "country")).cast(IntegerType()),
    )

    cols = ["location_key", "city", "state", "zip_code", "country"]
    return df.select(*cols)


def build_fact_order(spark: SparkSession) -> DataFrame:
    log.info("Building fact_order...")
    orders = spark.read.format("delta").load(str(SILVER_DATA_DIR / "orders"))
    customers = spark.read.format("delta").load(str(GOLD_DATA_DIR / "dim_customer"))
    products = spark.read.format("delta").load(str(GOLD_DATA_DIR / "dim_product"))

    lookup_cust = customers.select("customer_id", "customer_key").distinct()
    lookup_prod = products.select("product_id", "product_key").distinct()

    fact = (orders.alias("o")
            .join(lookup_cust.alias("c"), col("o.customer_id") == col("c.customer_id"), "left")
            .join(lookup_prod.alias("p"), col("o.product_id") == col("p.product_id"), "left"))

    fact = fact.select(
        col("o.order_id"),
        col("c.customer_key"),
        col("p.product_key"),
        to_date(col("o.order_date")).alias("date_key"),
        col("o.quantity"),
        col("o.unit_price"),
        col("o.total_amount"),
        col("o.order_status"),
    )

    return fact


def build_fact_payment(spark: SparkSession) -> DataFrame:
    log.info("Building fact_payment...")
    payments = spark.read.format("delta").load(str(SILVER_DATA_DIR / "payments"))
    orders = spark.read.format("delta").load(str(GOLD_DATA_DIR / "fact_order"))

    lookup_order = orders.select("order_id").distinct()
    lookup_order = lookup_order.withColumnRenamed("order_id", "fact_order_id")

    fact = (payments.alias("pay")
            .join(lookup_order.alias("o"), col("pay.order_id") == col("o.fact_order_id"), "left"))

    fact = fact.select(
        col("pay.payment_id"),
        col("pay.order_id"),
        to_date(col("pay.payment_date")).alias("date_key"),
        col("pay.amount"),
        col("pay.payment_method"),
        col("pay.payment_status"),
    )

    return fact


def build_fact_delivery(spark: SparkSession) -> DataFrame:
    log.info("Building fact_delivery...")
    deliveries = spark.read.format("delta").load(str(SILVER_DATA_DIR / "deliveries"))
    orders = spark.read.format("delta").load(str(GOLD_DATA_DIR / "fact_order"))

    lookup_order = orders.select("order_id").distinct()
    lookup_order = lookup_order.withColumnRenamed("order_id", "fact_order_id")

    fact = (deliveries.alias("d")
            .join(lookup_order.alias("o"), col("d.order_id") == col("o.fact_order_id"), "left"))

    fact = fact.select(
        col("d.delivery_id"),
        col("d.order_id"),
        to_date(col("d.delivery_date")).alias("date_key"),
        col("d.delivery_status"),
        col("d.estimated_days"),
        col("d.actual_days"),
        col("d.carrier"),
    )

    return fact


def write_gold(df: DataFrame, name: str):
    path = str(GOLD_DATA_DIR / name)
    log.info("Writing gold.%s (%s rows)", name, df.count())
    df.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(path)


def run_gold(spark: SparkSession | None = None) -> list[dict]:
    if spark is None:
        from spark_session import get_spark_session
        spark = get_spark_session("GoldLayer")

    GOLD_DATA_DIR.mkdir(parents=True, exist_ok=True)
    results = []

    # Dimensions (no dependencies)
    for name, fn in [("dim_date", build_dim_date), ("dim_customer", build_dim_customer),
                     ("dim_product", build_dim_product), ("dim_location", build_dim_location)]:
        t0 = time.time()
        df = fn(spark)
        write_gold(df, name)
        count = df.count()
        elapsed = round(time.time() - t0, 1)
        results.append({"table": name, "rows": count, "elapsed": elapsed})
        log.info("  ✓ gold.%s: %s rows in %.1fs", name, count, elapsed)

    # Facts (depend on dimensions)
    for name, fn in [("fact_order", build_fact_order), ("fact_payment", build_fact_payment),
                     ("fact_delivery", build_fact_delivery)]:
        t0 = time.time()
        df = fn(spark)
        write_gold(df, name)
        count = df.count()
        elapsed = round(time.time() - t0, 1)
        results.append({"table": name, "rows": count, "elapsed": elapsed})
        log.info("  ✓ gold.%s: %s rows in %.1fs", name, count, elapsed)

    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    t0 = time.time()
    results = run_gold()
    total_rows = sum(r["rows"] for r in results)
    log.info("Gold layer complete: %s rows across %s tables in %.1fs",
             total_rows, len(results), time.time() - t0)
