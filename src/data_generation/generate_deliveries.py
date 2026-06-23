import csv
import random
from pathlib import Path

from faker import Faker

from config import RAW_DATA_DIR

fake = Faker()
Faker.seed(42)
random.seed(42)

CARRIERS = ["FedEx", "UPS", "USPS", "DHL", "Amazon Logistics"]
DELIVERY_STATUSES = ["Shipped", "In Transit", "Delivered", "Returned", "Lost"]


def generate_deliveries(
    row_count: int = 500_000,
    order_ids: list[str] | None = None,
) -> Path:
    output_dir = RAW_DATA_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / "deliveries.csv"

    fields = [
        "delivery_id",
        "order_id",
        "delivery_date",
        "carrier",
        "tracking_number",
        "delivery_status",
        "estimated_days",
        "actual_days",
    ]

    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(fields)

        for _ in range(row_count):
            delivery_id = fake.uuid4()

            # 1% orphan order_id
            if order_ids and random.random() < 0.01:
                ord_id = fake.uuid4()
            else:
                ord_id = random.choice(order_ids) if order_ids else fake.uuid4()

            # 3% missing delivery_date
            if random.random() < 0.03:
                delivery_date = None
            else:
                delivery_date = fake.date_between(start_date="-4y", end_date="today")

            carrier = random.choice(CARRIERS)
            tracking_number = fake.bothify(text="??#########??", letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ")
            status = random.choice(DELIVERY_STATUSES)
            estimated_days = random.randint(1, 14)

            # actual_days should be null if not delivered
            if status == "Delivered":
                actual_days = random.randint(1, estimated_days + 5)
            else:
                actual_days = None

            writer.writerow(
                [
                    delivery_id,
                    ord_id,
                    delivery_date,
                    carrier,
                    tracking_number,
                    status,
                    estimated_days,
                    actual_days,
                ]
            )

    return filepath
