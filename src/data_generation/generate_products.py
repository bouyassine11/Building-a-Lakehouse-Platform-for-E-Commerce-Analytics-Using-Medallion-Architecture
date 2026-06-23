import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

from config import RAW_DATA_DIR

fake = Faker()
Faker.seed(42)
random.seed(42)

CATEGORIES = {
    "Electronics": ["Smartphones", "Laptops", "Accessories", "Audio", "Cameras"],
    "Clothing": ["Men", "Women", "Kids", "Shoes", "Accessories"],
    "Home & Garden": ["Furniture", "Kitchen", "Decor", "Tools", "Garden"],
    "Books": ["Fiction", "Non-Fiction", "Educational", "Comics", "Magazines"],
    "Sports": ["Fitness", "Outdoor", "Team Sports", "Cycling", "Swimming"],
}


def generate_products(row_count: int = 5_000) -> Path:
    output_dir = RAW_DATA_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / "products.csv"

    fields = [
        "product_id",
        "product_name",
        "category",
        "sub_category",
        "price",
        "cost",
        "supplier",
        "stock_quantity",
        "added_date",
    ]

    start_date = datetime(2020, 1, 1)
    end_date = datetime(2024, 12, 31)

    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(fields)

        for _ in range(row_count):
            product_id = fake.uuid4()
            product_name = fake.catch_phrase()

            # 3% chance of missing category
            if random.random() < 0.03:
                category = None
                sub_category = None
            else:
                category = random.choice(list(CATEGORIES.keys()))
                sub_category = random.choice(CATEGORIES[category])

            # 2% chance of invalid price (negative or zero)
            if random.random() < 0.02:
                price = round(random.uniform(-50, 0), 2)
            else:
                price = round(random.uniform(5, 2000), 2)

            cost = round(price * random.uniform(0.3, 0.8), 2)

            supplier = fake.company()

            # 1% negative stock
            stock = (
                -random.randint(1, 100)
                if random.random() < 0.01
                else random.randint(0, 10000)
            )

            added = fake.date_between(start_date=start_date, end_date=end_date)

            writer.writerow(
                [
                    product_id,
                    product_name,
                    category,
                    sub_category,
                    price,
                    cost,
                    supplier,
                    stock,
                    added,
                ]
            )

    return filepath
