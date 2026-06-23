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


def random_date(start: datetime, end: datetime) -> datetime:
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds()))
    )


def generate_customers(row_count: int = 50_000) -> Path:
    output_dir = RAW_DATA_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / "customers.csv"

    fields = [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "address",
        "city",
        "state",
        "zip_code",
        "country",
        "registration_date",
        "loyalty_tier",
        "birth_date",
    ]

    tiers = ["Bronze", "Silver", "Gold", "Platinum"]
    start_date = datetime(2018, 1, 1)
    end_date = datetime(2024, 12, 31)

    # Pre-generate some customer IDs for duplication
    base_ids = [fake.uuid4() for _ in range(row_count)]

    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(fields)

        for i in range(row_count):
            # Decide if this should be a duplicate (5% chance)
            if i > 0 and random.random() < 0.05:
                cust_id = base_ids[random.randint(0, i - 1)]
            else:
                cust_id = base_ids[i]

            first_name = fake.first_name()
            last_name = fake.last_name()

            # 3% chance of null email
            email = None if random.random() < 0.03 else fake.email()

            phone = fake.phone_number()
            address = fake.street_address()
            city = fake.city()
            state = fake.state()
            zip_code = fake.zipcode()
            country = fake.country()

            reg_date = random_date(start_date, end_date)
            # Random date format
            reg_date_str = random.choice(DATE_FORMATS)(reg_date)

            tier = random.choice(tiers)

            birth = random_date(datetime(1950, 1, 1), datetime(2005, 12, 31))
            birth_str = birth.strftime("%Y-%m-%d")

            writer.writerow(
                [
                    cust_id,
                    first_name,
                    last_name,
                    email,
                    phone,
                    address,
                    city,
                    state,
                    zip_code,
                    country,
                    reg_date_str,
                    tier,
                    birth_str,
                ]
            )

    return filepath
