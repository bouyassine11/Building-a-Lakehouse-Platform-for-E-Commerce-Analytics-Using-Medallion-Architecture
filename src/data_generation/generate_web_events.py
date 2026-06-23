import json
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

from config import RAW_DATA_DIR

fake = Faker()
Faker.seed(42)
random.seed(42)

PAGE_URLS = [
    "/",
    "/products",
    "/products/{category}",
    "/products/{category}/{product_id}",
    "/cart",
    "/checkout",
    "/account",
    "/account/orders",
    "/search?q={query}",
    "/about",
    "/contact",
]

EVENT_TYPES = ["page_view", "add_to_cart", "remove_from_cart", "begin_checkout", "purchase", "search", "account_login", "account_logout"]

DEVICE_TYPES = ["Desktop", "Mobile", "Tablet"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge", "Opera"]

CATEGORIES = ["electronics", "clothing", "home-garden", "books", "sports"]


def generate_web_events(
    row_count: int = 200_000,
    product_ids: list[str] | None = None,
) -> Path:
    output_dir = RAW_DATA_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / "web_events.json"

    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)

    visitors = [fake.uuid4() for _ in range(row_count // 20)]
    sessions: dict[str, datetime] = {}

    with open(filepath, "w") as f:
        for _ in range(row_count):
            visitor_id = random.choice(visitors)
            session_id = fake.uuid4()

            event_time = fake.date_time_between(start_date=start_date, end_date=end_date)
            event_type = random.choice(EVENT_TYPES)

            if product_ids and random.random() < 0.6:
                prod_id = random.choice(product_ids)
            else:
                prod_id = None

            if event_type == "search":
                url = "/search?q=" + fake.word()
            elif prod_id and random.random() < 0.5:
                category = random.choice(CATEGORIES)
                url = f"/products/{category}/{prod_id}"
            else:
                url = random.choice(PAGE_URLS).format(
                    category=random.choice(CATEGORIES),
                    product_id=random.choice(product_ids) if product_ids else "",
                    query=fake.word(),
                )

            device = random.choice(DEVICE_TYPES)
            browser = random.choice(BROWSERS)

            event = {
                "event_id": fake.uuid4(),
                "visitor_id": visitor_id,
                "session_id": session_id,
                "event_type": event_type,
                "page_url": url,
                "product_id": prod_id,
                "timestamp": event_time.isoformat(),
                "device_type": device,
                "browser": browser,
            }

            f.write(json.dumps(event) + "\n")

    return filepath
