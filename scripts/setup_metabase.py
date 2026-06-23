#!/usr/bin/env python3
"""Auto-configure Metabase: admin user + SparkSQL database connection."""

import json
import os
import sys
import time
import urllib.request
import urllib.error

METABASE_URL = os.environ.get("MB_URL", "http://localhost:3000")
MB_USER = os.environ.get("MB_USER", "admin@example.com")
MB_PASS = os.environ.get("MB_PASS", "admin123")
MB_SITE_NAME = os.environ.get("MB_SITE_NAME", "E-Commerce Lakehouse")

SPARK_HOST = os.environ.get("SPARK_HOST", "spark-thrift")
SPARK_PORT = os.environ.get("SPARK_PORT", "10000")
SPARK_DB = os.environ.get("SPARK_DB", "default")


def http(path, data=None, method="GET", token=None):
    url = f"{METABASE_URL}{path}"
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Metabase-Session"] = token
    body = json.dumps(data).encode() if data else None
    if body:
        method = "POST"
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:200]
        print(f"  HTTP {e.code} on {method} {path}: {body}", file=sys.stderr)
        return None


def wait_for_metabase(max_retries=30, interval=3):
    for i in range(max_retries):
        try:
            resp = http("/api/health")
            if resp and resp.get("status") == "ok":
                print("  Metabase is ready")
                return True
        except Exception:
            pass
        print(f"  Waiting for Metabase... ({i + 1}/{max_retries})")
        time.sleep(interval)
    return False


def setup_metabase():
    if not wait_for_metabase():
        print("ERROR: Metabase did not become ready", file=sys.stderr)
        sys.exit(1)

    props = http("/api/session/properties")
    if not props:
        print("ERROR: Cannot fetch Metabase properties", file=sys.stderr)
        sys.exit(1)

    setup_token = props.get("setup-token")
    if not setup_token:
        print("  Metabase already has an admin user")
        session = http("/api/session", data={"username": MB_USER, "password": MB_PASS})
        if not session:
            print("ERROR: Cannot login to existing Metabase", file=sys.stderr)
            sys.exit(1)
        token = session.get("id")

        dbs = http("/api/database", token=token)
        existing = [d for d in (dbs.get("data") or []) if d.get("engine") == "sparksql"]
        if existing:
            print(f"  SparkSQL database already configured (id={existing[0]['id']})")
            return token

        print("  Adding SparkSQL database...")
        db = http(
            "/api/database",
            data={
                "engine": "sparksql",
                "name": "Gold Layer (SparkSQL)",
                "details": {
                    "host": SPARK_HOST,
                    "port": int(SPARK_PORT),
                    "dbname": SPARK_DB,
                    "user": "",
                    "password": "",
                    "ssl": False,
                },
                "is_full_sync": True,
            },
            token=token,
        )
        if db:
            print(f"  SparkSQL database created (id={db.get('id')})")
        else:
            print("WARNING: Failed to create SparkSQL database (may need manual setup)")
        return token

    print("  Performing initial Metabase setup...")
    result = http(
        "/api/setup",
        data={
            "token": setup_token,
            "user": {
                "first_name": "Admin",
                "last_name": "User",
                "email": MB_USER,
                "password": MB_PASS,
            },
            "prefs": {
                "site_name": MB_SITE_NAME,
                "allow_tracking": False,
            },
        },
    )
    if not result:
        print("ERROR: Metabase setup failed", file=sys.stderr)
        sys.exit(1)

    token = result.get("id")
    print(f"  Admin user created ({MB_USER} / {MB_PASS})")

    db = http(
        "/api/database",
        data={
            "engine": "sparksql",
            "name": "Gold Layer (SparkSQL)",
            "details": {
                "host": SPARK_HOST,
                "port": int(SPARK_PORT),
                "dbname": SPARK_DB,
                "user": "",
                "password": "",
                "ssl": False,
            },
            "is_full_sync": True,
        },
        token=token,
    )
    if db:
        print(f"  SparkSQL database created (id={db.get('id')})")
    else:
        print("WARNING: Failed to create SparkSQL database (may need manual setup)")

    return token


def print_queries():
    GPT = "/opt/airflow/data/gold"
    print(
        f"""
──  Quick-start: register Gold tables  ──────────────────────────

Run these SQL statements once per Metabase question via the SQL editor
to create temporary views for readable table names:

  CREATE OR REPLACE TEMP VIEW dim_customer USING delta OPTIONS (path '{GPT}/dim_customer');
  CREATE OR REPLACE TEMP VIEW dim_product  USING delta OPTIONS (path '{GPT}/dim_product');
  CREATE OR REPLACE TEMP VIEW dim_date     USING delta OPTIONS (path '{GPT}/dim_date');
  CREATE OR REPLACE TEMP VIEW dim_location USING delta OPTIONS (path '{GPT}/dim_location');
  CREATE OR REPLACE TEMP VIEW fact_order   USING delta OPTIONS (path '{GPT}/fact_order');
  CREATE OR REPLACE TEMP VIEW fact_payment USING delta OPTIONS (path '{GPT}/fact_payment');
  CREATE OR REPLACE TEMP VIEW fact_delivery USING delta OPTIONS (path '{GPT}/fact_delivery');

Or query directly with backtick path syntax:

  SELECT * FROM delta.`{GPT}/dim_customer` LIMIT 10;

──  Sample analytical queries  ──────────────────────────────────

  -- Top 10 customers by total spend
  SELECT dc.customer_id,
         CONCAT(dc.first_name, ' ', dc.last_name) AS full_name,
         SUM(fo.total_amount) AS total_spend
  FROM fact_order fo
  JOIN dim_customer dc ON fo.customer_key = dc.customer_key
  GROUP BY dc.customer_id, dc.first_name, dc.last_name
  ORDER BY total_spend DESC
  LIMIT 10;

  -- Monthly revenue trend
  SELECT dd.year, dd.month, SUM(fo.total_amount) AS revenue
  FROM fact_order fo
  JOIN dim_date dd ON fo.date_key = dd.date_key
  GROUP BY dd.year, dd.month
  ORDER BY dd.year, dd.month;

  -- Revenue by product category
  SELECT dp.category, ROUND(SUM(fo.total_amount), 2) AS revenue
  FROM fact_order fo
  JOIN dim_product dp ON fo.product_key = dp.product_key
  GROUP BY dp.category
  ORDER BY revenue DESC;

  -- Payment success rate by method
  SELECT payment_method,
         COUNT(*) AS total,
         SUM(CASE WHEN payment_status = 'completed' THEN 1 ELSE 0 END) AS completed,
         ROUND(SUM(CASE WHEN payment_status = 'completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS success_pct
  FROM fact_payment
  GROUP BY payment_method
  ORDER BY success_pct DESC;

  -- Delivery performance (actual vs estimated days)
  SELECT carrier,
         ROUND(AVG(estimated_days), 1) AS avg_estimated,
         ROUND(AVG(actual_days), 1) AS avg_actual,
         ROUND(AVG(actual_days - estimated_days), 1) AS avg_delay
  FROM fact_delivery
  GROUP BY carrier
  ORDER BY avg_delay;
"""
    )


if __name__ == "__main__":
    token = setup_metabase()
    print_queries()
    print("Metabase ready at", METABASE_URL)
