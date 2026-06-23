from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

# Data layer paths
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_DATA_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DATA_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DATA_DIR = PROJECT_ROOT / "data" / "gold"

# Source data configuration
SOURCE_CONFIG = {
    "customers": {
        "file": "customers.csv",
        "format": "csv",
        "row_count": 10_000,
    },
    "products": {
        "file": "products.csv",
        "format": "csv",
        "row_count": 1_000,
    },
    "orders": {
        "file": "orders.csv",
        "format": "csv",
        "row_count": 50_000,
    },
    "payments": {
        "file": "payments.csv",
        "format": "csv",
        "row_count": 50_000,
    },
    "deliveries": {
        "file": "deliveries.csv",
        "format": "csv",
        "row_count": 50_000,
    },
    "web_events": {
        "file": "web_events.json",
        "format": "json",
        "row_count": 20_000,
    },
}

# Bronze layer table names (map source -> bronze table)
BRONZE_TABLES = [name for name in SOURCE_CONFIG]

# Silver layer table names
SILVER_TABLES = ["customers", "products", "orders", "payments", "deliveries"]

# Gold layer table names
GOLD_DIMENSION_TABLES = ["dim_customer", "dim_product", "dim_date", "dim_location"]
GOLD_FACT_TABLES = ["fact_order", "fact_payment", "fact_delivery"]
GOLD_TABLES = GOLD_DIMENSION_TABLES + GOLD_FACT_TABLES

# Quality thresholds
QUALITY_CONFIG = {
    "max_null_fraction": 0.05,
    "max_duplicate_fraction": 0.02,
    "max_fk_violation_fraction": 0.01,
}

# Date range for dim_date
DATE_START = "2020-01-01"
DATE_END = "2026-12-31"
