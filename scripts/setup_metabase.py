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
MB_PASS = os.environ.get("MB_PASS", "Metabase!2026")
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
    if body and method == "GET":
        method = "POST"
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:200]
        print(f"  HTTP {e.code} on {method} {path}: {body}", file=sys.stderr)
        return None


def wait_for_metabase(max_retries=120, interval=3):
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
    # try to log in first in case setup already exists
    session = http("/api/session", data={"username": MB_USER, "password": MB_PASS})
    if session:
        print("  Metabase already has an admin user")
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
                "is_full_sync": False,
            },
            token=token,
        )
        if db:
            db_id = db.get("id")
            print(f"  SparkSQL database created (id={db_id})")
            # trigger sync in the background (non-blocking)
            http(f"/api/database/{db_id}/sync", method="POST", token=token)
            http(f"/api/database/{db_id}/rescan_values", method="POST", token=token)
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
            "is_full_sync": False,
        },
        token=token,
    )
    if db:
        db_id = db.get("id")
        print(f"  SparkSQL database created (id={db_id})")
        http(f"/api/database/{db_id}/sync", method="POST", token=token)
        http(f"/api/database/{db_id}/rescan_values", method="POST", token=token)
    else:
        print("WARNING: Failed to create SparkSQL database (may need manual setup)")

    return token


GPT = "/opt/airflow/data/gold"

def p(path):
    return f"delta.`{GPT}/{path}`"

FO = p("fact_order")
DC = p("dim_customer")
DD = p("dim_date")
DP = p("dim_product")
FP = p("fact_payment")
FD = p("fact_delivery")
DL = p("dim_location")

QUESTIONS = [
    {
        "name": "Top 10 Customers by Total Spend",
        "display": "bar",
        "query": f"""
            SELECT dc.customer_id,
                   CONCAT(dc.first_name, ' ', dc.last_name) AS full_name,
                   ROUND(SUM(fo.total_amount), 2) AS total_spend
            FROM {FO} fo
            JOIN {DC} dc ON fo.customer_key = dc.customer_key
            GROUP BY dc.customer_id, dc.first_name, dc.last_name
            ORDER BY total_spend DESC
            LIMIT 10
        """,
        "viz": {"graph.dimensions": ["full_name"], "graph.metrics": ["total_spend"]},
    },
    {
        "name": "Monthly Revenue Trend",
        "display": "line",
        "query": f"""
            SELECT CONCAT(CAST(dd.year AS STRING), '-', LPAD(CAST(dd.month AS STRING), 2, '0')) AS month,
                   ROUND(SUM(fo.total_amount), 2) AS revenue
            FROM {FO} fo
            JOIN {DD} dd ON fo.date_key = dd.date_key
            GROUP BY dd.year, dd.month
            ORDER BY dd.year, dd.month
        """,
        "viz": {"graph.dimensions": ["month"], "graph.metrics": ["revenue"]},
    },
    {
        "name": "Revenue by Product Category",
        "display": "bar",
        "query": f"""
            SELECT dp.category, ROUND(SUM(fo.total_amount), 2) AS revenue
            FROM {FO} fo
            JOIN {DP} dp ON fo.product_key = dp.product_key
            GROUP BY dp.category
            ORDER BY revenue DESC
        """,
        "viz": {"graph.dimensions": ["category"], "graph.metrics": ["revenue"]},
    },
    {
        "name": "Payment Success Rate by Method",
        "display": "bar",
        "query": f"""
            SELECT payment_method,
                   COUNT(*) AS total,
                   SUM(CASE WHEN payment_status = 'completed' THEN 1 ELSE 0 END) AS completed,
                   ROUND(SUM(CASE WHEN payment_status = 'completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS success_pct
            FROM {FP}
            GROUP BY payment_method
            ORDER BY success_pct DESC
        """,
        "viz": {"graph.dimensions": ["payment_method"], "graph.metrics": ["success_pct"]},
    },
    {
        "name": "Delivery Performance by Carrier",
        "display": "bar",
        "query": f"""
            SELECT carrier,
                   ROUND(AVG(estimated_days), 1) AS avg_estimated,
                   ROUND(AVG(actual_days), 1) AS avg_actual,
                   ROUND(AVG(actual_days - estimated_days), 1) AS avg_delay
            FROM {FD}
            GROUP BY carrier
            ORDER BY avg_delay
        """,
        "viz": {"graph.dimensions": ["carrier"], "graph.metrics": ["avg_estimated", "avg_actual", "avg_delay"]},
    },
]


def get_db_id(token):
    dbs = http("/api/database", token=token)
    for d in (dbs.get("data") or []):
        if d.get("engine") == "sparksql":
            return d["id"]
    return None


def create_questions(token, db_id):
    ids = []
    for q in QUESTIONS:
        card = http(
            "/api/card",
            data={
                "name": q["name"],
                "display": q["display"],
                "dataset_query": {
                    "type": "native",
                    "native": {"query": q["query"], "template-tags": {}},
                    "database": db_id,
                },
                "visualization_settings": q["viz"],
            },
            token=token,
        )
        if card:
            cid = card.get("id")
            ids.append(cid)
            print(f"  Created question '{q['name']}' (id={cid})")
        else:
            print(f"  WARNING: Failed to create question '{q['name']}'")
    return ids


def create_dashboard(token, card_ids):
    dash = http(
        "/api/dashboard",
        data={"name": "E-Commerce Analytics", "description": "Key metrics from the Gold star schema"},
        token=token,
    )
    if not dash:
        print("  WARNING: Failed to create dashboard")
        return
    did = dash.get("id")
    print(f"  Created dashboard 'E-Commerce Analytics' (id={did})")

    layout = [(0, 0, 6, 4), (6, 0, 6, 4), (0, 4, 4, 4), (4, 4, 4, 4), (8, 4, 4, 4)]
    dashcards = [
        {"id": -(i + 1), "card_id": cid, "col": c, "row": r, "size_x": sx, "size_y": sy}
        for i, (cid, (c, r, sx, sy)) in enumerate(zip(card_ids, layout))
    ]
    result = http(
        f"/api/dashboard/{did}",
        data={"dashcards": dashcards},
        method="PUT",
        token=token,
    )
    n = len(result.get("dashcards", [])) if result else 0
    print(f"  Added {n} cards to dashboard")
    return did


if __name__ == "__main__":
    token = setup_metabase()
    if not token:
        print("ERROR: Metabase setup failed", file=sys.stderr)
        sys.exit(1)

    db_id = get_db_id(token)
    if not db_id:
        print("  WARNING: SparkSQL database not found, skipping dashboard creation")
    else:
        print(f"  Found SparkSQL database (id={db_id})")
        card_ids = create_questions(token, db_id)
        if card_ids:
            create_dashboard(token, card_ids)

    print("Metabase ready at", METABASE_URL)
