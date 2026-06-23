import os
import sys
from pathlib import Path

_proj = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_proj))

import pytest
from pyspark.sql import SparkSession

# Ensure compatible Java version for Spark
_JAVA_CANDIDATES = [
    "/usr/lib/jvm/java-21-openjdk-amd64",
    "/usr/lib/jvm/java-17-openjdk-amd64",
]
if "JAVA_HOME" not in os.environ:
    for jh in _JAVA_CANDIDATES:
        if Path(jh).is_dir():
            os.environ["JAVA_HOME"] = jh
            break


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("JDK_JAVA_OPTIONS", "--add-opens java.base/javax.security.auth=ALL-UNNAMED")
    session = (
        SparkSession.builder.appName("test")
        .master("local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
