"""
Generates realistic e-commerce transaction data for the Lakehouse pipeline.
Includes intentional data quality issues for DQ testing.
"""
import random
import uuid
import csv
import json
from datetime import datetime, timedelta


PRODUCTS = [
    {"id": "P001", "name": "Laptop Pro 15", "category": "Electronics", "price": 1299.99},
    {"id": "P002", "name": "Wireless Mouse", "category": "Electronics", "price": 29.99},
    {"id": "P003", "name": "USB-C Hub", "category": "Accessories", "price": 49.99},
    {"id": "P004", "name": "Standing Desk", "category": "Furniture", "price": 599.99},
    {"id": "P005", "name": "Monitor 27inch", "category": "Electronics", "price": 399.99},
    {"id": "P006", "name": "Keyboard MX", "category": "Electronics", "price": 99.99},
    {"id": "P007", "name": "Webcam HD", "category": "Electronics", "price": 79.99},
    {"id": "P008", "name": "Desk Lamp", "category": "Furniture", "price": 45.99},
]

COUNTRIES = ["FR", "MA", "US", "UK", "DE", "ES", None]
PAYMENT_METHODS = ["credit_card", "paypal", "bank_transfer", "crypto", ""]


def generate_transactions(n: int = 10000, error_rate: float = 0.05) -> list:
    """Generate e-commerce transactions with controlled data quality issues."""
    transactions = []
    base_date = datetime(2024, 1, 1)

    for i in range(n):
        product = random.choice(PRODUCTS)
        qty = random.randint(1, 5)
        inject_error = random.random() < error_rate

        tx = {
            "transaction_id": str(uuid.uuid4()),
            "customer_id": f"C{random.randint(1, 500):05d}",
            "product_id": product["id"],
            "product_name": product["name"],
            "category": product["category"],
            "quantity": -qty if inject_error else qty,
            "unit_price": product["price"],
            "total_amount": round(product["price"] * qty, 2),
            "currency": "EUR",
            "payment_method": random.choice(PAYMENT_METHODS),
            "country": random.choice(COUNTRIES),
            "order_date": (base_date + timedelta(
                days=random.randint(0, 180),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )).isoformat(),
            "is_returned": random.random() < 0.08,
        }

        if random.random() < 0.02 and transactions:
            transactions.append(transactions[-1].copy())
        else:
            transactions.append(tx)

    return transactions


if __name__ == "__main__":
    random.seed(42)
    data = generate_transactions(50000)
    with open("data/sample/transactions_sample.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data[:1000])
    print(f"Generated {len(data)} transactions, sample (1000 rows) saved to data/sample/transactions_sample.csv")
