# E-Commerce Data Engineering Pipeline: Quality Validation, Idempotency and Reconciliation

A clean, practical Data Engineering pipeline in Python for ingesting, validating, and publishing e-commerce order records into SQLite with automated reconciliation, quarantine tracking, and comprehensive test coverage.

---

## 1. Problem Statement

E-commerce order feeds often suffer from data quality issues:
- Duplicate order submissions caused by network retries or client re-clicks.
- Missing mandatory fields (such as `order_id`, `customer_id`, or `amount`).
- Malformed dates and unparseable timestamps.
- Negative order amounts or non-numeric values.
- Unknown customer IDs not found in the customer master table.
- Invalid order statuses.
- Malformed CSV rows with uneven delimiters or formatting errors.

If unvalidated data enters reporting or analytics databases, it leads to inaccurate financial metrics and broken downstream queries. This pipeline validates every record against business rules, isolates bad records with clear rejection reasons, ensures referential integrity, and guarantees idempotent database publishing.

---

## 2. Project Objectives

- Automated Quality Validation: Validate mandatory fields, ISO dates, non-negative amounts, allowed order statuses, and customer references.
- Malformed CSV Handling: Detect and quarantine malformed CSV lines without silently discarding records.
- Referential Integrity: Enforce foreign key constraints between `orders` and `customers` in SQLite using `PRAGMA foreign_keys = ON`.
- Idempotency: Prevent duplicate order insertion when re-running the same input dataset.
- Rejection Quarantine Layer: Record rejected and malformed records in `rejected_orders.csv` with source file, source row number, run ID, timestamp, and rejection reason.
- Persistent Pipeline History: Track every execution run in a `pipeline_runs` SQLite table.
- Source-to-Target Reconciliation: Produce automated reconciliation reports mathematically proving row counts and database consistency.
- High Test Coverage: Provide 35 Pytest test scenarios covering happy paths, edge cases, schema drift, foreign key enforcement, retry recovery, and 100K-record benchmarks.

---

## 3. Technology Stack

- Python 3.10+
- Pandas (tabular data handling and CSV exports)
- SQLite 3 (embedded relational database with foreign key support)
- Pytest (automated testing framework)
- Python Built-in Modules: `csv`, `sqlite3`, `json`, `logging`, `argparse`, `time`, `datetime`, `os`, `sys`, `random`

---

## 4. Architecture and Pipeline Flow

```
+---------------------------------------------------------------------------------------+
|                                  Pipeline Flow                                        |
+---------------------------------------------------------------------------------------+
|                                                                                       |
|   1. INGESTION       Read raw CSV line-by-line; capture malformed rows with line numbers |
|                                |                                                      |
|   2. VALIDATION      Check: order_id presence & intra-batch uniqueness                |
|                             customer_id reference validity                            |
|                             order_date ISO-8601 calendar format                       |
|                             amount non-negative numeric (>= 0.00)                     |
|                             order_status in ('completed', 'pending', 'cancelled')     |
|                                |                                                      |
|   3. SEPARATION     [Valid Records]                 [Rejected Records]                |
|                             |                                |                        |
|   4. TRANSFORMATION  Standardize dates and status,  Preserve raw values, capture row  |
|                      add run_id & processed timestamp number & rejection reason       |
|                             |                                |                        |
|   5. PERSISTENCE     clean_orders.csv               rejected_orders.csv (Appends/     |
|                      orders table in SQLite         preserves run history)            |
|                      (Foreign keys ON, idempotency)                                   |
|                             |                                                         |
|   6. HISTORY         Record execution stats in pipeline_runs table                    |
|                             |                                                         |
|   7. RECONCILIATION  Generate quality_report.json, reconciliation_report.txt,         |
|                      and performance_report.txt                                       |
+---------------------------------------------------------------------------------------+
```

---

## 5. Source and Target Schema

### Source CSV Schema (`data/raw/orders.csv`)
- `order_id` (String): Order identifier.
- `customer_id` (String): Customer identifier.
- `order_date` (String): Transaction date (ISO-8601).
- `amount` (String/Float): Transaction value.
- `order_status` (String): Status (`completed`, `pending`, `cancelled`).

### Clean Orders Output (`data/processed/clean_orders.csv`)
- `order_id`, `customer_id`, `order_date`, `amount`, `order_status`, `source_file`, `run_id`, `processed_at`.

### Quarantined Rejected Output (`data/rejected/rejected_orders.csv`)
- `run_id`, `source_file`, `source_row_number`, `order_id`, `customer_id`, `order_date`, `amount`, `order_status`, `rejection_reason`, `rejected_at`.

### SQLite Target Database (`database/orders.db`)
- `customers` table: `customer_id TEXT PRIMARY KEY`
- `orders` table: `order_id TEXT PRIMARY KEY`, `customer_id TEXT NOT NULL REFERENCES customers(customer_id)`, `order_date TEXT NOT NULL`, `amount REAL NOT NULL`, `order_status TEXT NOT NULL`, `source_file TEXT NOT NULL`, `run_id TEXT NOT NULL`, `processed_at TEXT NOT NULL`
- `pipeline_runs` table: `run_id TEXT PRIMARY KEY`, `source_file TEXT`, `start_time TEXT`, `end_time TEXT`, `status TEXT`, `input_count INTEGER`, `valid_count INTEGER`, `rejected_count INTEGER`, `inserted_count INTEGER`, `duplicate_count INTEGER`, `failure_reason TEXT`

---

## 6. Business Rules

1. Order ID: Required, non-empty, unique business key.
2. Customer ID: Required, must exist in `customers` table (`CUST0001` through `CUST0100`).
3. Order Date: Required, valid ISO-8601 calendar date (`YYYY-MM-DD` or timestamp). Supports leap years and out-of-order dates.
4. Order Amount: Required, numeric, non-negative. Zero amount (`0.00`) is permitted for promotions.
5. Order Status: Case-insensitive, must normalize to `completed`, `pending`, or `cancelled`.

---

## 7. Duplicate Handling

- Intra-Batch Duplicates: Identified using `seen_batch_orders`. The first occurrence is processed; subsequent rows with the same `order_id` in the same file are quarantined.
- Exact Database Duplicates: When re-running the same dataset, orders already present in SQLite with identical details are identified and skipped without error.
- Conflicting Duplicates: If an order arrives with an `order_id` that already exists in the database but with different customer, date, or amount, it is quarantined and the existing database row is preserved.

---

## 8. Validation and Rejection Process

Records that fail any validation rule or CSV formatting check are written to `data/rejected/rejected_orders.csv`.
- Line number is recorded in `source_row_number`.
- Raw unparsed values are preserved.
- Human-readable reason is attached (e.g. `Invalid customer_id: 'CUST9999' not found in reference list`).
- Historical rejections from prior runs are appended and preserved.

---

## 9. Idempotency and Retry Behavior

- Re-running the pipeline on identical input produces zero duplicate rows in SQLite (`inserted_count = 0`).
- Database publishing is executed inside an atomic transaction; incomplete transactions roll back on failure.
- Retrying after resolving an issue safely inserts only uncommitted records.

---

## 10. Database Design

SQLite is configured with:
```sql
PRAGMA foreign_keys = ON;
```
Tables:
1. `customers`: Stores reference customer IDs.
2. `orders`: Target table with `order_id` as primary key and foreign key constraint referencing `customers(customer_id)`.
3. `pipeline_runs`: Execution audit log with status `SUCCESS` or `FAILED`, row counts, and error descriptions.

---

## 11. Installation Instructions

### Prerequisites
- Python 3.10 or higher.
- `pip` package manager.

### Setup Steps
```bash
# Navigate to project folder
cd data_engineering_project

# Optional: Create and activate virtual environment
python -m venv venv
# On Windows:
.\venv\Scripts\activate
# On Linux/macOS:
source venv/bin/activate

# Install required dependencies
pip install -r requirements.txt
```

---

## 12. How to Generate Sample Data

Use `generate_data.py` to create reproducible synthetic data with controlled anomalies:

```bash
# Generate 1,000 sample records (default)
python generate_data.py --rows 1000 --seed 42 --output data/raw/orders.csv

# Generate 10,000 records
python generate_data.py --rows 10000 --seed 42 --output data/raw/orders.csv
```

---

## 13. How to Run the Pipeline

Run the pipeline using default paths:
```bash
python main.py
```

Run with custom paths:
```bash
python main.py --input data/raw/orders.csv --clean-output data/processed/clean_orders.csv --rejected-output data/rejected/rejected_orders.csv --database database/orders.db
```

---

## 14. How to Run Automated Tests

Run all 35 test scenarios via Pytest:
```bash
pytest -v
```

Or run using python module:
```bash
python -m pytest tests/test_pipeline.py -v
```

### Test Suite Overview (35 Scenarios)
- Scenarios 1–10: Happy path, empty file, missing fields, malformed values, batch duplicates, null checks, negative/zero amounts, invalid statuses.
- Scenarios 11–20: Customer references, unexpected schema changes, out-of-order dates, duplicate re-runs, partial failure/retry, primary key uniqueness, reconciliation formulas, regression checks, duplicate ordering, timestamps.
- Scenarios 21–30: Amount edge cases, conflicting database orders, malformed CSV input, database failure recovery, report content verifications, rejected traceability, 100K-record benchmark.
- Scenarios 31–35: Foreign key enforcement, persistent pipeline run history, malformed row quarantine with row numbers, conflicting duplicate detection, transaction rollback on publishing failure.

---

## 15. How to Execute the 100K-Record Benchmark

Generate a 100,000-record dataset and execute the pipeline:
```bash
# Generate 100,000 records
python generate_data.py --rows 100000 --seed 42 --output data/raw/orders.csv

# Run pipeline
python main.py
```

---

## 16. Expected Outputs

After execution, the following files are produced:
1. `data/processed/clean_orders.csv`: Validated orders for current run.
2. `data/rejected/rejected_orders.csv`: Quarantined orders with row numbers and rejection reasons.
3. `database/orders.db`: SQLite database containing `orders`, `customers`, and `pipeline_runs`.
4. `reports/quality_report.json`: JSON metrics and rejection reason breakdown.
5. `reports/reconciliation_report.txt`: Row count reconciliation and target audit report.
6. `reports/performance_report.txt`: Execution time, throughput, and stage breakdown.
7. `logs/pipeline.log`: Timestamped operational log entries.

---

## 17. Reconciliation Explanation

Reconciliation checks ensure no records are lost or unaccounted for:
1. Row Count Check:
   `Total Source Records = Valid Records + Rejected Records`
   Rejected records include malformed CSV lines, schema violations, invalid data values, and batch duplicates.
2. Target Database Check:
   `Post-Run Database Count = Pre-Run Database Count + Newly Inserted Records`
3. Idempotency Check:
   `Valid Records = Newly Inserted Records + Exact Duplicates Skipped`

---

## 18. Actual Performance Benchmark Results

Measured on 100,000 synthetic records using `time.perf_counter()`:
- Number of input records: 100,000
- Number of valid records: 83,129
- Number of rejected records: 16,871 (including 1,026 malformed rows + 15,845 validation failures)
- Processing time: ~2.50 seconds
- Throughput: ~39,955 rows/second
- Final target count in database: 83,129
- Row reconciliation check: PASSED
- Database reconciliation check: PASSED

---

## 19. SQL Verification Queries

To independently verify the SQLite database:

```sql
-- 1. Check total orders in database
SELECT COUNT(*) AS total_orders FROM orders;

-- 2. Check for duplicate order IDs (should return 0 rows)
SELECT order_id, COUNT(*) FROM orders GROUP BY order_id HAVING COUNT(*) > 1;

-- 3. Check referential integrity (should return 0 rows)
SELECT o.order_id, o.customer_id
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- 4. Check pipeline execution history
SELECT run_id, source_file, status, inserted_count, duplicate_count FROM pipeline_runs ORDER BY start_time DESC;

-- 5. Financial total by order status
SELECT order_status, COUNT(*), ROUND(SUM(amount), 2) FROM orders GROUP BY order_status;
```

---

## 20. Known Limitations

- Single-Node File Database: SQLite handles up to ~1,000,000 records smoothly for single-process jobs. High-concurrency multi-writer environments would require PostgreSQL or a cloud data warehouse.
- In-Memory Batching: Ingests the batch in memory. Datasets exceeding single-machine RAM (> 5 million records) should use chunked reading.
- Customer Master Reference: Uses a synthetic reference list (`CUST0001` through `CUST0100`).

---

## 21. Future Improvements

- Streaming Ingestion: Add support for reading large files in chunks (`chunksize`) for low-memory environments.
- Cloud Storage: Support reading directly from AWS S3 or Google Cloud Storage buckets.
- Workflow Orchestration: Schedule recurring runs using Airflow or cron.
- Monitoring & Alerts: Send webhook notifications when quarantine percentages exceed predefined thresholds.
