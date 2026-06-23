from pathlib import Path

import pytest

from config import RAW_DATA_DIR, BRONZE_DATA_DIR, SILVER_DATA_DIR, GOLD_DATA_DIR, SOURCE_CONFIG


def test_e2e_data_generated():
    for name, cfg in SOURCE_CONFIG.items():
        f = RAW_DATA_DIR / cfg["file"]
        assert f.exists(), f"Missing source file: {f}"
        size = f.stat().st_size
        assert size > 0, f"Source file {f} is empty"


def test_e2e_bronze_tables_exist():
    for name in SOURCE_CONFIG:
        path = BRONZE_DATA_DIR / name
        assert path.exists(), f"Missing bronze table: {path}"
        delta_log = path / "_delta_log"
        assert delta_log.exists(), f"Missing Delta log for bronze.{name}"
        log_files = list(delta_log.glob("*.json"))
        assert len(log_files) > 0, f"Empty Delta log for bronze.{name}"


def test_e2e_silver_tables_exist():
    for name in ["customers", "products", "orders", "payments", "deliveries", "web_events"]:
        path = SILVER_DATA_DIR / name
        assert path.exists(), f"Missing silver table: {path}"
        delta_log = path / "_delta_log"
        assert delta_log.exists(), f"Missing Delta log for silver.{name}"


def test_e2e_gold_tables_exist():
    gold_tables = ["dim_date", "dim_customer", "dim_product", "dim_location",
                   "fact_order", "fact_payment", "fact_delivery"]
    for name in gold_tables:
        path = GOLD_DATA_DIR / name
        assert path.exists(), f"Missing gold table: {path}"
        delta_log = path / "_delta_log"
        assert delta_log.exists(), f"Missing Delta log for gold.{name}"


@pytest.mark.skipif(
    not BRONZE_DATA_DIR.exists(),
    reason="Bronze data not available"
)
def test_e2e_bronze_row_counts():
    spark = _get_spark()
    expected = {k: v["row_count"] for k, v in SOURCE_CONFIG.items()}
    for name in SOURCE_CONFIG:
        df = spark.read.format("delta").load(str(BRONZE_DATA_DIR / name))
        count = df.count()
        assert count >= expected[name], f"bronze.{name}: expected >= {expected[name]}, got {count}"


@pytest.mark.skipif(
    not SILVER_DATA_DIR.exists(),
    reason="Silver data not available"
)
def test_e2e_silver_row_counts():
    spark = _get_spark()
    for name in ["customers", "products", "orders", "payments", "deliveries", "web_events"]:
        df = spark.read.format("delta").load(str(SILVER_DATA_DIR / name))
        count = df.count()
        assert count > 0, f"silver.{name} is empty"


def _get_spark():
    from src.bronze.ingest_to_bronze import run_bronze
    from spark_session import get_spark_session
    return get_spark_session("TestE2E")
