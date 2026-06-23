"""
Medallion Architecture ETL Pipeline

Runs the full data pipeline: generate synthetic data -> Bronze ingestion
-> Silver transformation -> Gold star schema.

Dag runs daily at 2am. Each step is idempotent (mode=overwrite or append).
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import SOURCE_CONFIG

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": 300,
    "email_on_failure": True,
}


def run_generate_data(**context) -> dict:
    from src.data_generation.generate_all import generate_all
    return generate_all()


def run_bronze(**context) -> list[dict]:
    from src.bronze.ingest_to_bronze import run_bronze
    return run_bronze()


def run_silver(**context) -> list[dict]:
    from src.silver.transform_to_silver import run_silver
    return run_silver()


def run_gold(**context) -> list[dict]:
    from src.gold.build_gold_star_schema import run_gold
    return run_gold()


with DAG(
    dag_id="medallion_pipeline",
    default_args=DEFAULT_ARGS,
    description="End-to-end medallion architecture ETL pipeline",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["medallion", "bronze", "silver", "gold"],
    doc_md=__doc__,
) as dag:

    generate_data = PythonOperator(
        task_id="generate_data",
        python_callable=run_generate_data,
        doc_md="Generate synthetic e-commerce source data into data/raw/",
    )

    bronze = PythonOperator(
        task_id="bronze",
        python_callable=run_bronze,
        doc_md="Ingest raw CSV/JSON into Bronze Delta tables with audit columns",
    )

    silver = PythonOperator(
        task_id="silver",
        python_callable=run_silver,
        doc_md="Clean, deduplicate, standardize, and validate FK into Silver layer",
    )

    gold = PythonOperator(
        task_id="gold",
        python_callable=run_gold,
        doc_md="Build dimensional star schema (dim_* + fact_*) in Gold layer",
    )

    generate_data >> bronze >> silver >> gold
