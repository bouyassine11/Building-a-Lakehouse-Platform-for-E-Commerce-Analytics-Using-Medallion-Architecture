import csv
import random
from pathlib import Path

from faker import Faker

from config import RAW_DATA_DIR

fake = Faker()
Faker.seed(42)
random.seed(42)

PAYMENT_METHODS = ["Credit Card", "PayPal", "Debit Card", "Bank Transfer", "Gift Card"]
PAYMENT_STATUSES = ["Completed", "Pending", "Failed", "Refunded"]


def generate_payments(
    row_count: int = 500_000,
    order_ids: list[str] | None = None,
) -> Path:
    output_dir = RAW_DATA_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / "payments.csv"

    fields = [
        "payment_id",
        "order_id",
        "payment_date",
        "amount",
        "payment_method",
        "payment_status",
        "transaction_id",
    ]

    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(fields)

        for _ in range(row_count):
            payment_id = fake.uuid4()

            # 2% chance of orphan order_id
            if order_ids and random.random() < 0.02:
                ord_id = fake.uuid4()
            else:
                ord_id = random.choice(order_ids) if order_ids else fake.uuid4()

            payment_date = fake.date_between(start_date="-4y", end_date="today")

            # 1% null amount
            if random.random() < 0.01:
                amount = None
            else:
                amount = round(random.uniform(5, 5000), 2)

            # 2% missing method
            method = (
                None
                if random.random() < 0.02
                else random.choice(PAYMENT_METHODS)
            )

            status = random.choice(PAYMENT_STATUSES)
            transaction_id = fake.uuid4()

            writer.writerow(
                [
                    payment_id,
                    ord_id,
                    payment_date,
                    amount,
                    method,
                    status,
                    transaction_id,
                ]
            )

    return filepath
