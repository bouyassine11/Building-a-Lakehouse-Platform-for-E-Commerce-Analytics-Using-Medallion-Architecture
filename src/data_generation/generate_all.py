import logging
import sys
import time
from pathlib import Path

# Ensure project root is on sys.path
_project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_project_root))

from config import RAW_DATA_DIR, SOURCE_CONFIG

from src.data_generation.generate_customers import generate_customers
from src.data_generation.generate_products import generate_products
from src.data_generation.generate_orders import generate_orders
from src.data_generation.generate_payments import generate_payments
from src.data_generation.generate_deliveries import generate_deliveries
from src.data_generation.generate_web_events import generate_web_events

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)


def generate_all() -> dict[str, Path]:
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Generating customers (%s rows)...", SOURCE_CONFIG["customers"]["row_count"])
    t0 = time.time()
    customers_path = generate_customers(SOURCE_CONFIG["customers"]["row_count"])
    log.info("Done in %.1fs: %s", time.time() - t0, customers_path)

    log.info("Generating products (%s rows)...", SOURCE_CONFIG["products"]["row_count"])
    t0 = time.time()
    products_path = generate_products(SOURCE_CONFIG["products"]["row_count"])
    log.info("Done in %.1fs: %s", time.time() - t0, products_path)

    log.info("Generating orders (%s rows)...", SOURCE_CONFIG["orders"]["row_count"])
    t0 = time.time()
    orders_path = generate_orders(SOURCE_CONFIG["orders"]["row_count"])
    log.info("Done in %.1fs: %s", time.time() - t0, orders_path)

    log.info("Generating payments (%s rows)...", SOURCE_CONFIG["payments"]["row_count"])
    t0 = time.time()
    payments_path = generate_payments(SOURCE_CONFIG["payments"]["row_count"])
    log.info("Done in %.1fs: %s", time.time() - t0, payments_path)

    log.info("Generating deliveries (%s rows)...", SOURCE_CONFIG["deliveries"]["row_count"])
    t0 = time.time()
    deliveries_path = generate_deliveries(SOURCE_CONFIG["deliveries"]["row_count"])
    log.info("Done in %.1fs: %s", time.time() - t0, deliveries_path)

    log.info("Generating web_events (%s rows)...", SOURCE_CONFIG["web_events"]["row_count"])
    t0 = time.time()
    web_events_path = generate_web_events(SOURCE_CONFIG["web_events"]["row_count"])
    log.info("Done in %.1fs: %s", time.time() - t0, web_events_path)

    return {
        "customers": customers_path,
        "products": products_path,
        "orders": orders_path,
        "payments": payments_path,
        "deliveries": deliveries_path,
        "web_events": web_events_path,
    }


if __name__ == "__main__":
    paths = generate_all()
    log.info("All data generated successfully:")
    for name, path in paths.items():
        size_mb = path.stat().st_size / (1024 * 1024)
        log.info("  %s: %s (%.1f MB)", name, path, size_mb)
