import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

from config import RAW_DATA_DIR

fake = Faker()
Faker.seed(42)
random.seed(42)

DATE_FORMATS = [
    lambda d: d.strftime("%Y-%m-%d"),
    lambda d: d.strftime("%m/%d/%Y"),
    lambda d: d.strftime("%d-%m-%Y"),
    lambda d: d.strftime("%Y%m%d"),
]

ORDER_STATUSES = ["Pending", "Processing", "Shipped", "Delivered", "Cancelled", "Returned"]


def generate_orders(
    row_count: int = 500_000,
    customer_ids: list[str] | None = None,
    product_ids: list[str] | None = None,
) -> Path:
    output_dir = RAW_DATA_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / "orders.csv"

    fields = [
        "order_id",
        "customer_id",
        "product_id",
        "order_date",
        "quantity",
        "unit_price",
        "total_amount",
        "order_status",
        "shipping_address",
    ]

    start_date = datetime(2020, 1, 1)
    end_date = datetime(2024, 12, 31)

    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(fields)

        for _ in range(row_count):
            order_id = fake.uuid4()

            # 2% chance of orphan customer
            if customer_ids and random.random() < 0.02:
                cust_id = fake.uuid4()
            else:
                cust_id = random.choice(customer_ids) if customer_ids else fake.uuid4()

            prod_id = random.choice(product_ids) if product_ids else fake.uuid4()

            order_date = fake.date_between(start_date=start_date, end_date=end_date)
            order_date_str = random.choice(DATE_FORMATS)(order_date)

            # 1% negative/zero quantity
            if random.random() < 0.01:
                quantity = random.randint(-5, 0)
            else:
                quantity = random.randint(1, 10)

            unit_price = round(random.uniform(5, 2000), 2)
            total_amount = round(quantity * unit_price, 2)

            # 1% missing status
            status = None if random.random() < 0.01 else random.choice(ORDER_STATUSES)

            address = fake.address().replace("\n", ", ")

            writer.writerow(
                [
                    order_id,
                    cust_id,
                    prod_id,
                    order_date_str,
                    quantity,
                    unit_price,
                    total_amount,
                    status,
                    address,
                ]
            )

    return filepath
