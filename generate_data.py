# Synthetic data generator for e-commerce order pipeline
# Generates reproducible datasets with both valid and invalid records

import os
import csv
import random
import argparse
from datetime import datetime, timedelta

# List of known valid customer IDs for the project
KNOWN_CUSTOMERS = [f"CUST{i:04d}" for i in range(1, 101)]

# Allowed valid order statuses
VALID_STATUSES = ["completed", "pending", "cancelled"]

# Invalid statuses used for testing validation failure
INVALID_STATUSES = ["refunded", "failed", "processing", "shipped"]


def generate_orders_dataset(output_path, total_rows=1000, random_seed=42):
    # Set fixed random seed for reproducible datasets
    random.seed(random_seed)

    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # Base date range for orders (over the last 180 days)
    start_date = datetime(2024, 1, 1)

    headers = ["order_id", "customer_id", "order_date", "amount", "order_status"]
    records = []

    for i in range(1, total_rows + 1):
        # Default valid values
        order_id = f"ORD{i:06d}"
        customer_id = random.choice(KNOWN_CUSTOMERS)

        # Random dates create naturally out-of-order timestamps
        random_days = random.randint(0, 180)
        order_date = (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d")

        amount = f"{round(random.uniform(5.0, 500.0), 2):.2f}"

        # Valid status with mixed casing to test normalization
        raw_status = random.choice(VALID_STATUSES)
        if random.random() < 0.2:
            order_status = raw_status.upper()
        elif random.random() < 0.2:
            order_status = raw_status.capitalize()
        else:
            order_status = raw_status

        # Inject controlled anomalies based on random probability
        anomaly_type = random.random()

        if anomaly_type < 0.02 and i > 5:
            # Duplicate order ID from earlier in the batch
            earlier_index = random.randint(1, i - 1)
            order_id = f"ORD{earlier_index:06d}"
        elif anomaly_type < 0.04:
            # Missing order ID
            order_id = ""
        elif anomaly_type < 0.06:
            # Missing customer ID
            customer_id = ""
        elif anomaly_type < 0.08:
            # Invalid customer ID not in reference list
            customer_id = f"CUST9{random.randint(100, 999)}"
        elif anomaly_type < 0.10:
            # Malformed date
            order_date = random.choice(["invalid-date", "2024-13-45", "not_a_date", ""])
        elif anomaly_type < 0.12:
            # Missing amount
            amount = ""
        elif anomaly_type < 0.14:
            # Negative amount
            amount = f"-{round(random.uniform(1.0, 100.0), 2):.2f}"
        elif anomaly_type < 0.16:
            # Zero amount (valid boundary condition)
            amount = "0.00"
        elif anomaly_type < 0.18:
            # Invalid order status
            order_status = random.choice(INVALID_STATUSES)
        elif anomaly_type < 0.19:
            # Malformed CSV row with uneven columns
            records.append([order_id, customer_id, order_date, amount, order_status, "corrupt_extra_col"])
            continue

        records.append([order_id, customer_id, order_date, amount, order_status])

    # Write records to CSV file using standard csv module
    file_handle = open(output_path, mode="w", newline="", encoding="utf-8")
    writer = csv.writer(file_handle)
    writer.writerow(headers)
    writer.writerows(records)
    file_handle.close()

    print(f"Generated {total_rows} records successfully at: {output_path}")
    return output_path


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce orders dataset")
    parser.add_argument("--rows", type=int, default=1000, help="Number of records to generate (default: 1000)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility (default: 42)")
    parser.add_argument("--output", type=str, default="data/raw/orders.csv", help="Output file path (default: data/raw/orders.csv)")

    args = parser.parse_args()
    generate_orders_dataset(output_path=args.output, total_rows=args.rows, random_seed=args.seed)


if __name__ == "__main__":
    main()
