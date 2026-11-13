# Data Dictionary: E-Commerce Data Engineering Pipeline

## 1. Overview

This document describes the data models, column definitions, constraints, business validation rules, and execution metadata for the E-Commerce Data Engineering Pipeline.

---

## 2. Source Entity (`data/raw/orders.csv`)

The raw orders CSV file is ingested from upstream sales channels.

| Column Name | Data Type | Nullable | Key Type | Description | Example Values |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `order_id` | String | No | Primary Key | Unique business identifier for the order | `ORD000001`, `ORD000520` |
| `customer_id` | String | No | Foreign Key | Customer identifier placing the order | `CUST0001`, `CUST0045` |
| `order_date` | String | No | None | Date or timestamp of the order | `2024-05-15`, `2024-01-20T14:30:00` |
| `amount` | String / Numeric | No | None | Monetary order value | `149.99`, `0.00` |
| `order_status` | String | No | None | Operational order status | `completed`, `pending`, `cancelled` |

---

## 3. Cleansed Processed Entity (`data/processed/clean_orders.csv`)

Contains valid orders that passed all business quality checks for the current run.

| Column Name | Data Type | Nullable | Key Type | Transformation / Derivation | Example Values |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `order_id` | String | No | Primary Key | Trimmed, validated unique | `ORD000001` |
| `customer_id` | String | No | Foreign Key | Validated against customers reference table | `CUST0001` |
| `order_date` | String | No | None | Standardized to ISO-8601 calendar date (`YYYY-MM-DD`) | `2024-05-15` |
| `amount` | Float | No | None | Converted to numeric float rounded to 2 decimal places | `149.99`, `0.00` |
| `order_status` | String | No | None | Normalized to lowercase (`completed`, `pending`, `cancelled`) | `completed`, `pending` |
| `source_file` | String | No | None | Input CSV filename for audit lineage | `orders.csv` |
| `run_id` | String | No | None | Execution batch identifier | `RUN_20261113_104053_653599` |
| `processed_at` | String | No | None | ISO processing timestamp (`YYYY-MM-DD HH:MM:SS`) | `2026-11-13 10:40:53` |

---

## 4. Quarantined Rejected Entity (`data/rejected/rejected_orders.csv`)

Captures all records that failed ingestion or validation. Previous execution rejects are preserved in this file.

| Column Name | Data Type | Nullable | Description | Example Values |
| :--- | :--- | :--- | :--- | :--- |
| `run_id` | String | No | Execution ID during which quarantine occurred | `RUN_20261113_104053_653599` |
| `source_file` | String | No | Source filename where record originated | `orders.csv` |
| `source_row_number` | Integer | No | Exact line number in raw CSV file (header is line 1) | `3`, `45` |
| `order_id` | String | Yes | Raw order ID as received from source | `ORD000100`, `""` |
| `customer_id` | String | Yes | Raw customer ID as received from source | `CUST9999`, `""` |
| `order_date` | String | Yes | Raw order date as received from source | `2024-13-45`, `""` |
| `amount` | String | Yes | Raw amount as received from source | `-25.00`, `abc` |
| `order_status` | String | Yes | Raw status as received from source | `refunded`, `shipped` |
| `rejection_reason` | String | No | Specific validation failure reason | See rejection list below |
| `rejected_at` | String | No | Timestamp when record was quarantined | `2026-11-13 10:40:53` |

### Quarantined Rejection Reasons

- `Malformed CSV record: expected 5 columns, found <N>`
- `Malformed CSV syntax error: <details>`
- `Missing mandatory field: order_id`
- `Duplicate order_id within input batch: '<order_id>'`
- `Missing mandatory field: customer_id`
- `Invalid customer_id: '<customer_id>' not found in reference list`
- `Missing mandatory field: order_date`
- `Invalid order_date: '<date>' is not a valid ISO date`
- `Missing amount`
- `Invalid amount: negative values not allowed`
- `Invalid amount: not a numeric value`
- `Missing mandatory field: order_status`
- `Invalid order_status: '<status>' must be completed, pending, or cancelled`
- `Conflicting duplicate: order_id '<order_id>' already exists in database with different details`

---

## 5. Target SQLite Database Tables (`database/orders.db`)

### Table 1: `customers` (Reference Table)

Maintains the master list of valid customer reference IDs for relational foreign-key integrity.

| Column Name | SQLite Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `customer_id` | `TEXT` | `PRIMARY KEY` | Unique customer reference ID (`CUST0001` to `CUST0100`) |

### Table 2: `orders` (Target Orders Table)

Stores cleansed, validated orders permanently. Foreign key enforcement is active via `PRAGMA foreign_keys = ON`.

| Column Name | SQLite Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `order_id` | `TEXT` | `PRIMARY KEY` | Unique natural business key for the order |
| `customer_id` | `TEXT` | `NOT NULL, FOREIGN KEY REFERENCES customers(customer_id)` | Customer ID placing the order |
| `order_date` | `TEXT` | `NOT NULL` | Standardized ISO-8601 calendar date (`YYYY-MM-DD`) |
| `amount` | `REAL` | `NOT NULL` | Financial monetary order amount (>= 0.00) |
| `order_status` | `TEXT` | `NOT NULL` | Normalized status: `completed`, `pending`, `cancelled` |
| `source_file` | `TEXT` | `NOT NULL` | Name of raw input file |
| `run_id` | `TEXT` | `NOT NULL` | Run ID when record was inserted |
| `processed_at` | `TEXT` | `NOT NULL` | Processing timestamp (`YYYY-MM-DD HH:MM:SS`) |

### Table 3: `pipeline_runs` (Execution History Table)

Stores persistent history of all pipeline executions without overwriting previous runs.

| Column Name | SQLite Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `run_id` | `TEXT` | `PRIMARY KEY` | Unique run identifier |
| `source_file` | `TEXT` | `NOT NULL` | Input filename processed |
| `start_time` | `TEXT` | `NOT NULL` | ISO-8601 run start timestamp |
| `end_time` | `TEXT` | `NOT NULL` | ISO-8601 run completion timestamp |
| `status` | `TEXT` | `NOT NULL` | Run outcome: `SUCCESS` or `FAILED` |
| `input_count` | `INTEGER` | `NOT NULL` | Total source records read |
| `valid_count` | `INTEGER` | `NOT NULL` | Count of records passing validation |
| `rejected_count` | `INTEGER` | `NOT NULL` | Count of quarantined records |
| `inserted_count` | `INTEGER` | `NOT NULL` | Newly inserted target records |
| `duplicate_count` | `INTEGER` | `NOT NULL` | Total duplicate orders identified |
| `failure_reason` | `TEXT` | `NULL` | Error details if status is `FAILED` |

---

## 6. Business Validation Rules

1. **Order Identifier Uniqueness (`order_id`)**:
   - Must be present and non-empty.
   - Must be unique across the batch and across the database.
   - First occurrence in an input batch is accepted; subsequent occurrences in the same batch are quarantined as batch duplicates.
   - If an order already exists in the database with identical details, it is skipped as an exact duplicate (idempotent re-run).
   - If an order already exists in the database with different customer, date, or amount, it is quarantined as a conflicting duplicate.

2. **Customer Master Reference (`customer_id`)**:
   - Must be present and non-empty.
   - Must match an existing record in the `customers` table (`CUST0001` through `CUST0100`).
   - SQLite enforces this via `FOREIGN KEY (customer_id) REFERENCES customers(customer_id)` with `PRAGMA foreign_keys = ON`.

3. **Order Date Format (`order_date`)**:
   - Must be a valid ISO-8601 calendar date (`YYYY-MM-DD` or `YYYY-MM-DDTHH:MM:SS`).
   - Validates correct month (1–12), day bounds per month, and leap years (Feb 29 only in leap years).
   - Out-of-order dates are permitted without dropping valid historical orders.

4. **Order Amount (`amount`)**:
   - Must be a valid numeric float (rejects negative numbers, text, `NaN`, `inf`).
   - Boundary value `$0.00` is allowed for promotions, free trials, and voucher orders.
   - Rounded to 2 decimal places.

5. **Order Status Normalization (`order_status`)**:
   - Case-insensitive comparison, normalized to lowercase.
   - Must be one of: `completed`, `pending`, `cancelled`.
