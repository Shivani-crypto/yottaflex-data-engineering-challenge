# Automated Test Suite for E-Commerce Data Engineering Pipeline
# Contains all 18 required test scenarios using Pytest with isolated temporary directories

import os
import sys
import sqlite3
import pandas as pd

# Add project root directory to sys.path so main module can be imported seamlessly
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from main import run_pipeline, is_valid_iso_date, is_valid_amount


# Scenario 1: Happy path with valid records
def test_scenario_01_happy_path_valid_records(tmp_path):
    csv_file = tmp_path / "valid_orders.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD001,CUST0001,2024-05-01,150.00,completed\n"
        "ORD002,CUST0002,2024-05-02,75.50,PENDING\n"
        "ORD003,CUST0003,2024-05-03,20.00,Cancelled\n"
    )

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    metrics = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001", "CUST0002", "CUST0003"],
    )

    assert metrics["execution_status"] == "SUCCESS"
    assert metrics["total_records_read"] == 3
    assert metrics["valid_records_count"] == 3
    assert metrics["rejected_records_count"] == 0
    assert metrics["inserted_records_count"] == 3
    assert metrics["total_valid_amount"] == 245.50

    # Verify SQLite database content
    conn = sqlite3.connect(str(db_file))
    cursor = conn.cursor()
    cursor.execute("SELECT order_id, order_status FROM orders ORDER BY order_id")
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 3
    assert rows[0] == ("ORD001", "completed")
    assert rows[1] == ("ORD002", "pending")
    assert rows[2] == ("ORD003", "cancelled")


# Scenario 2: Empty input file
def test_scenario_02_empty_input_file(tmp_path):
    empty_csv = tmp_path / "empty_orders.csv"
    empty_csv.write_text("")

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    metrics = run_pipeline(
        input_path=str(empty_csv),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
    )

    assert metrics["execution_status"] == "SUCCESS_EMPTY_FILE"
    assert metrics["total_records_read"] == 0
    assert metrics["valid_records_count"] == 0
    assert metrics["rejected_records_count"] == 0
    assert os.path.exists(str(clean_csv))
    assert os.path.exists(str(rejected_csv))


# Scenario 3: Missing required fields
def test_scenario_03_missing_required_fields(tmp_path):
    csv_file = tmp_path / "missing_fields.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        ",CUST0001,2024-05-01,100.00,completed\n"
        "ORD002,,2024-05-01,100.00,completed\n"
        "ORD003,CUST0001,,100.00,completed\n"
        "ORD004,CUST0001,2024-05-01,,completed\n"
        "ORD005,CUST0001,2024-05-01,100.00,\n"
        "ORD006,CUST0001,2024-05-01,100.00,completed\n"
    )

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    metrics = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    assert metrics["total_records_read"] == 6
    assert metrics["valid_records_count"] == 1
    assert metrics["rejected_records_count"] == 5

    rej_df = pd.read_csv(str(rejected_csv))
    reasons = list(rej_df["rejection_reason"])
    assert any("Missing mandatory field: order_id" in r for r in reasons)
    assert any("Missing mandatory field: customer_id" in r for r in reasons)
    assert any("Missing mandatory field: order_date" in r for r in reasons)
    assert any("Missing amount" in r for r in reasons)
    assert any("Missing mandatory field: order_status" in r for r in reasons)


# Scenario 4: Malformed records
def test_scenario_04_malformed_records(tmp_path):
    csv_file = tmp_path / "malformed_records.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD001,CUST0001,bad-date,50.00,completed\n"
        "ORD002,CUST0001,2024-05-01,not_a_number,completed\n"
        "ORD003,CUST0001,2024-05-01,50.00,unknown_status\n"
    )

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    metrics = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    assert metrics["valid_records_count"] == 0
    assert metrics["rejected_records_count"] == 3


# Scenario 5: Duplicate order IDs in input batch
def test_scenario_05_duplicate_order_ids(tmp_path):
    csv_file = tmp_path / "batch_duplicates.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD100,CUST0001,2024-05-01,10.00,completed\n"
        "ORD100,CUST0001,2024-05-02,20.00,pending\n"
        "ORD101,CUST0001,2024-05-03,30.00,completed\n"
    )

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    metrics = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    assert metrics["valid_records_count"] == 2
    assert metrics["rejected_records_count"] == 1
    assert metrics["batch_duplicate_count"] == 1

    rej_df = pd.read_csv(str(rejected_csv))
    assert "Duplicate order_id within input batch: 'ORD100'" in rej_df["rejection_reason"].values


# Scenario 6: Null customer IDs
def test_scenario_06_null_customer_ids(tmp_path):
    csv_file = tmp_path / "null_customers.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD201,,2024-05-01,50.00,completed\n"
        "ORD202,   ,2024-05-01,60.00,completed\n"
    )

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    metrics = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
    )

    assert metrics["valid_records_count"] == 0
    assert metrics["rejected_records_count"] == 2


# Scenario 7: Null amounts
def test_scenario_07_null_amounts(tmp_path):
    csv_file = tmp_path / "null_amounts.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD301,CUST0001,2024-05-01,,completed\n"
        "ORD302,CUST0001,2024-05-01,   ,completed\n"
    )

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    metrics = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    assert metrics["valid_records_count"] == 0
    assert metrics["rejected_records_count"] == 2


# Scenario 8: Negative amounts
def test_scenario_08_negative_amounts(tmp_path):
    csv_file = tmp_path / "negative_amounts.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD401,CUST0001,2024-05-01,-10.50,completed\n"
        "ORD402,CUST0001,2024-05-01,-0.01,pending\n"
    )

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    metrics = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    assert metrics["valid_records_count"] == 0
    assert metrics["rejected_records_count"] == 2
    rej_df = pd.read_csv(str(rejected_csv))
    for reason in rej_df["rejection_reason"]:
        assert "negative values not allowed" in reason


# Scenario 9: Zero amounts are allowed
def test_scenario_09_zero_amounts(tmp_path):
    csv_file = tmp_path / "zero_amounts.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD501,CUST0001,2024-05-01,0.00,completed\n"
        "ORD502,CUST0001,2024-05-02,0,pending\n"
    )

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    metrics = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    assert metrics["valid_records_count"] == 2
    assert metrics["rejected_records_count"] == 0
    assert metrics["total_valid_amount"] == 0.0


# Scenario 10: Invalid order statuses
def test_scenario_10_invalid_order_statuses(tmp_path):
    csv_file = tmp_path / "invalid_statuses.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD601,CUST0001,2024-05-01,15.00,shipped\n"
        "ORD602,CUST0001,2024-05-01,25.00,refunded\n"
        "ORD603,CUST0001,2024-05-01,35.00,in_progress\n"
    )

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    metrics = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    assert metrics["valid_records_count"] == 0
    assert metrics["rejected_records_count"] == 3


# Scenario 11: Invalid customer reference IDs
def test_scenario_11_invalid_customer_reference_ids(tmp_path):
    csv_file = tmp_path / "unknown_customers.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD701,CUST9999,2024-05-01,50.00,completed\n"
        "ORD702,UNKNOWN_USER,2024-05-01,60.00,pending\n"
    )

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    metrics = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_path := tmp_path / "test_orders.db"),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001", "CUST0002"],
    )

    assert metrics["valid_records_count"] == 0
    assert metrics["rejected_records_count"] == 2
    rej_df = pd.read_csv(str(rejected_csv))
    for reason in rej_df["rejection_reason"]:
        assert "not found in reference list" in reason


# Scenario 12: Unexpected schema changes
def test_scenario_12_unexpected_schema_changes(tmp_path):
    # Case A: Missing mandatory column
    missing_col_csv = tmp_path / "missing_col.csv"
    missing_col_csv.write_text(
        "order_id,customer_id,order_date,order_status\n"
        "ORD801,CUST0001,2024-05-01,completed\n"
    )

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    metrics_missing = run_pipeline(
        input_path=str(missing_col_csv),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )
    assert metrics_missing["execution_status"] == "FAILED_SCHEMA_VALIDATION"

    # Case B: Extra unexpected column (pipeline logs warning and ingests required columns)
    extra_col_csv = tmp_path / "extra_col.csv"
    extra_col_csv.write_text(
        "order_id,customer_id,order_date,amount,order_status,internal_notes\n"
        "ORD802,CUST0001,2024-05-01,40.00,completed,vip_customer\n"
    )

    metrics_extra = run_pipeline(
        input_path=str(extra_col_csv),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )
    assert metrics_extra["execution_status"] == "SUCCESS"
    assert metrics_extra["valid_records_count"] == 1


# Scenario 13: Out-of-order dates
def test_scenario_13_out_of_order_dates(tmp_path):
    csv_file = tmp_path / "unordered_dates.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD901,CUST0001,2024-12-25,99.00,completed\n"
        "ORD902,CUST0001,2024-01-05,50.00,completed\n"
        "ORD903,CUST0001,2024-06-15,75.00,completed\n"
    )

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    metrics = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    assert metrics["valid_records_count"] == 3
    assert metrics["inserted_records_count"] == 3

    conn = sqlite3.connect(str(db_file))
    cursor = conn.cursor()
    cursor.execute("SELECT order_date FROM orders ORDER BY order_date ASC")
    dates = [row[0] for row in cursor.fetchall()]
    conn.close()

    assert dates == ["2024-01-05", "2024-06-15", "2024-12-25"]


# Scenario 14: Duplicate rerun of the same input (idempotency)
def test_scenario_14_duplicate_rerun_idempotency(tmp_path):
    csv_file = tmp_path / "rerun_orders.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD1001,CUST0001,2024-05-01,10.00,completed\n"
        "ORD1002,CUST0001,2024-05-02,20.00,pending\n"
    )

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    # First run
    metrics_1 = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )
    assert metrics_1["inserted_records_count"] == 2
    assert metrics_1["duplicate_rerun_count"] == 0

    # Second run with identical input
    metrics_2 = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )
    assert metrics_2["inserted_records_count"] == 0
    assert metrics_2["duplicate_rerun_count"] == 2

    # Database still holds exactly 2 records
    conn = sqlite3.connect(str(db_file))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM orders")
    total_db = cursor.fetchone()[0]
    conn.close()

    assert total_db == 2


# Scenario 15: Partial failure and retry behavior
def test_scenario_15_partial_failure_and_retry(tmp_path):
    batch_1 = tmp_path / "batch_1.csv"
    batch_1.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD1101,CUST0001,2024-05-01,10.00,completed\n"
        "ORD1102,CUST0001,2024-05-02,-99.00,completed\n"
    )

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    # Run batch 1: ORD1101 succeeds, ORD1102 fails due to negative amount
    metrics_1 = run_pipeline(
        input_path=str(batch_1),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )
    assert metrics_1["valid_records_count"] == 1
    assert metrics_1["rejected_records_count"] == 1
    assert metrics_1["inserted_records_count"] == 1

    # Retry batch with fixed amount for ORD1102 plus original ORD1101
    batch_2 = tmp_path / "batch_2.csv"
    batch_2.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD1101,CUST0001,2024-05-01,10.00,completed\n"
        "ORD1102,CUST0001,2024-05-02,99.00,completed\n"
    )

    metrics_2 = run_pipeline(
        input_path=str(batch_2),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )
    assert metrics_2["valid_records_count"] == 2
    assert metrics_2["rejected_records_count"] == 0
    assert metrics_2["inserted_records_count"] == 1
    assert metrics_2["duplicate_rerun_count"] == 1

    # Database now contains both orders safely
    conn = sqlite3.connect(str(db_file))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM orders")
    total_db = cursor.fetchone()[0]
    conn.close()

    assert total_db == 2


# Scenario 16: Database primary key uniqueness
def test_scenario_16_database_primary_key_uniqueness(tmp_path):
    db_file = tmp_path / "test_pk.db"
    csv_file = tmp_path / "sample.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD1201,CUST0001,2024-05-01,50.00,completed\n"
    )

    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    # Inspect table schema using PRAGMA
    conn = sqlite3.connect(str(db_file))
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(orders)")
    columns = cursor.fetchall()
    conn.close()

    # Find order_id column and assert primary key flag is 1
    order_id_col = [col for col in columns if col[1] == "order_id"][0]
    is_primary_key = order_id_col[5]
    assert is_primary_key == 1


# Scenario 17: Source-to-target reconciliation
def test_scenario_17_source_to_target_reconciliation(tmp_path):
    csv_file = tmp_path / "recon_test.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD1301,CUST0001,2024-05-01,100.00,completed\n"
        "ORD1302,CUST0001,2024-05-02,50.00,pending\n"
        "ORD1303,CUST0001,2024-05-03,25.00,cancelled\n"
        "ORD1304,CUST0001,2024-05-04,-10.00,completed\n"
        "ORD1305,CUST9999,2024-05-05,40.00,completed\n"
        "ORD1301,CUST0001,2024-05-06,100.00,completed\n"
    )

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    metrics = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    # 6 source rows = 3 valid + 3 rejected (1 negative amount, 1 unknown customer, 1 duplicate ORD1301)
    assert metrics["total_records_read"] == 6
    assert metrics["valid_records_count"] == 3
    assert metrics["rejected_records_count"] == 3
    assert metrics["batch_duplicate_count"] == 1
    assert metrics["total_valid_amount"] == 175.00

    assert metrics["reconciliation"]["row_reconciliation_passed"] is True
    assert metrics["reconciliation"]["db_reconciliation_passed"] is True

    # Check status summary
    assert metrics["status_summary"]["completed"] == 1
    assert metrics["status_summary"]["pending"] == 1
    assert metrics["status_summary"]["cancelled"] == 1


# Scenario 18: Regression tests for previously identified issues
def test_scenario_18_regression_tests(tmp_path):
    # Regression A: ISO timestamp with T separator
    is_valid_t, clean_t = is_valid_iso_date("2024-08-15T18:45:00")
    assert is_valid_t is True
    assert clean_t == "2024-08-15"

    # Regression B: Leap year February 29th
    is_leap_valid, _ = is_valid_iso_date("2024-02-29")
    assert is_leap_valid is True

    is_non_leap_valid, _ = is_valid_iso_date("2023-02-29")
    assert is_non_leap_valid is False

    # Regression C: Amount decimal formatting and rounding
    is_amt_valid, amt_val, _ = is_valid_amount("89.999")
    assert is_amt_valid is True
    assert amt_val == 90.00

    # Regression D: Integer string amount
    is_int_valid, int_val, _ = is_valid_amount("120")
    assert is_int_valid is True
    assert int_val == 120.00


# Scenario 19: Duplicate order ID where first record is invalid and second record is valid
def test_scenario_19_duplicate_order_id_first_invalid_second_valid(tmp_path):
    # Row 1 has ORD999 with negative amount (-50.00)
    # Row 2 has ORD999 with valid amount (50.00)
    csv_file = tmp_path / "dup_invalid_then_valid.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD999,CUST0001,2024-05-01,-50.00,completed\n"
        "ORD999,CUST0001,2024-05-01,50.00,completed\n"
    )

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    metrics = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    # Business rule: order_id must be unique across the batch
    # Record 1 rejected for invalid negative amount
    # Record 2 rejected as duplicate order_id within batch
    assert metrics["total_records_read"] == 2
    assert metrics["valid_records_count"] == 0
    assert metrics["rejected_records_count"] == 2
    assert metrics["batch_duplicate_count"] == 1

    rej_df = pd.read_csv(str(rejected_csv))
    reasons = list(rej_df["rejection_reason"])
    assert any("negative values not allowed" in r for r in reasons)
    assert any("Duplicate order_id within input batch: 'ORD999'" in r for r in reasons)


# Scenario 20: Invalid timestamps, including invalid hours and minutes
def test_scenario_20_invalid_timestamps_hours_minutes(tmp_path):
    # Invalid hour 25
    is_valid_h, _ = is_valid_iso_date("2024-05-15T25:00:00")
    assert is_valid_h is False

    # Invalid minute 75
    is_valid_m, _ = is_valid_iso_date("2024-05-15T14:75:00")
    assert is_valid_m is False

    # Invalid second 80
    is_valid_s, _ = is_valid_iso_date("2024-05-15T14:30:80")
    assert is_valid_s is False

    # Test via pipeline with invalid timestamp rows and valid timestamp row
    csv_file = tmp_path / "timestamp_test.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD1401,CUST0001,2024-05-15T25:30:00,50.00,completed\n"
        "ORD1402,CUST0001,2024-05-15 14:70:00,60.00,completed\n"
        "ORD1403,CUST0001,2024-05-15T14:30:00,70.00,completed\n"
    )

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    metrics = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    assert metrics["valid_records_count"] == 1
    assert metrics["rejected_records_count"] == 2


# Scenario 21: Amount validation for .50, 10., NaN, inf, abc and 1.2.3
def test_scenario_21_amount_validation_edge_cases(tmp_path):
    # Test valid edge cases: .50 and 10.
    valid_dot_lead, val_lead, _ = is_valid_amount(".50")
    assert valid_dot_lead is True
    assert val_lead == 0.50

    valid_dot_trail, val_trail, _ = is_valid_amount("10.")
    assert valid_dot_trail is True
    assert val_trail == 10.00

    # Test invalid edge cases: NaN, inf, abc, 1.2.3
    valid_nan, _, _ = is_valid_amount("NaN")
    assert valid_nan is False

    valid_inf, _, _ = is_valid_amount("inf")
    assert valid_inf is False

    valid_abc, _, _ = is_valid_amount("abc")
    assert valid_abc is False

    valid_multi_dot, _, _ = is_valid_amount("1.2.3")
    assert valid_multi_dot is False

    # Pipeline integration test
    csv_file = tmp_path / "amount_edge_cases.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD1501,CUST0001,2024-05-01,.50,completed\n"
        "ORD1502,CUST0001,2024-05-02,10.,completed\n"
        "ORD1503,CUST0001,2024-05-03,NaN,completed\n"
        "ORD1504,CUST0001,2024-05-04,inf,completed\n"
        "ORD1505,CUST0001,2024-05-05,abc,completed\n"
        "ORD1506,CUST0001,2024-05-06,1.2.3,completed\n"
    )

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    metrics = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    assert metrics["total_records_read"] == 6
    assert metrics["valid_records_count"] == 2
    assert metrics["rejected_records_count"] == 4


# Scenario 22: Conflicting order IDs already present in SQLite with different order details
def test_scenario_22_conflicting_order_ids_already_in_sqlite(tmp_path):
    db_file = tmp_path / "test_conflict.db"

    # Pre-populate database with original order
    conn = sqlite3.connect(str(db_file))
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE orders ("
        "order_id TEXT PRIMARY KEY, "
        "customer_id TEXT NOT NULL, "
        "order_date TEXT NOT NULL, "
        "amount REAL NOT NULL, "
        "order_status TEXT NOT NULL, "
        "source_file TEXT NOT NULL, "
        "run_id TEXT NOT NULL, "
        "processed_at TEXT NOT NULL"
        ")"
    )
    cursor.execute(
        "INSERT INTO orders VALUES ('ORD1601', 'CUST0001', '2024-01-01', 100.00, 'completed', 'initial.csv', 'RUN_INITIAL', '2024-01-01 10:00:00')"
    )
    conn.commit()
    conn.close()

    # Incoming batch attempts to re-use ORD1601 with completely different details (different customer, date, amount, status)
    csv_file = tmp_path / "conflicting_orders.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD1601,CUST0002,2024-09-09,999.99,pending\n"
        "ORD1602,CUST0001,2024-05-02,50.00,completed\n"
    )

    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    metrics = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001", "CUST0002"],
    )

    assert metrics["inserted_records_count"] == 1
    assert metrics["duplicate_rerun_count"] == 1

    # Verify original order in database was preserved and not overwritten
    conn = sqlite3.connect(str(db_file))
    cursor = conn.cursor()
    cursor.execute("SELECT customer_id, amount, order_status FROM orders WHERE order_id = 'ORD1601'")
    row = cursor.fetchone()
    cursor.execute("SELECT COUNT(*) FROM orders")
    total_count = cursor.fetchone()[0]
    conn.close()

    assert row == ("CUST0001", 100.00, "completed")
    assert total_count == 2


# Scenario 23: Malformed CSV input
def test_scenario_23_malformed_csv_input(tmp_path):
    # CSV with uneven columns and malformed lines
    csv_file = tmp_path / "broken_format.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD1701,CUST0001,2024-05-01,25.00,completed\n"
        "CORRUPT_ROW_WITH_EXTRA_COMMAS,A,B,C,D,E,F,G\n"
        "ORD1702,CUST0001,2024-05-02,30.00,completed\n"
    )

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    metrics = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    # Malformed row is safely skipped by on_bad_lines='skip', valid rows processed
    assert metrics["execution_status"] == "SUCCESS"
    assert metrics["valid_records_count"] == 2
    assert metrics["inserted_records_count"] == 2


# Scenario 24: Simulated database failure and retry behavior
def test_scenario_24_simulated_database_failure_and_retry(tmp_path):
    csv_file = tmp_path / "retry_test.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD1801,CUST0001,2024-05-01,45.00,completed\n"
        "ORD1802,CUST0001,2024-05-02,55.00,pending\n"
    )

    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    # Step 1: Simulate database failure by passing a directory path instead of a file
    invalid_db_dir = tmp_path / "invalid_db_directory"
    os.makedirs(invalid_db_dir, exist_ok=True)

    metrics_failed = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(invalid_db_dir),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )
    assert metrics_failed["execution_status"] == "FAILED_DATABASE_ERROR"

    # Step 2: Retry pipeline execution with valid database path
    valid_db_file = tmp_path / "recovered_orders.db"
    metrics_retry = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(valid_db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    assert metrics_retry["execution_status"] == "SUCCESS"
    assert metrics_retry["valid_records_count"] == 2
    assert metrics_retry["inserted_records_count"] == 2

    conn = sqlite3.connect(str(valid_db_file))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM orders")
    total_db = cursor.fetchone()[0]
    conn.close()

    assert total_db == 2


# Scenario 25: Verification of quality_report.json contents
def test_scenario_25_quality_report_json_contents(tmp_path):
    import json

    csv_file = tmp_path / "orders.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD1901,CUST0001,2024-05-01,100.00,completed\n"
        "ORD1902,CUST0001,2024-05-02,-50.00,completed\n"
    )

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    file_handle = open(str(quality_json), "r", encoding="utf-8")
    report_data = json.load(file_handle)
    file_handle.close()

    assert report_data["run_id"].startswith("RUN_")
    assert report_data["source_file"] == "orders.csv"
    assert report_data["execution_status"] == "SUCCESS"
    assert report_data["total_records_read"] == 2
    assert report_data["valid_records_count"] == 1
    assert report_data["rejected_records_count"] == 1
    assert "status_summary" in report_data
    assert "rejection_breakdown" in report_data
    assert "reconciliation" in report_data
    assert report_data["reconciliation"]["row_reconciliation_passed"] is True
    assert report_data["reconciliation"]["db_reconciliation_passed"] is True


# Scenario 26: Verification of reconciliation report contents
def test_scenario_26_reconciliation_report_contents(tmp_path):
    csv_file = tmp_path / "orders.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD2001,CUST0001,2024-05-01,75.00,completed\n"
    )

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    file_handle = open(str(recon_txt), "r", encoding="utf-8")
    report_text = file_handle.read()
    file_handle.close()

    assert "E-COMMERCE DATA PIPELINE RECONCILIATION REPORT" in report_text
    assert "1. ROW COUNT RECONCILIATION" in report_text
    assert "2. TARGET DATABASE RECONCILIATION" in report_text
    assert "3. FINANCIAL & STATUS SUMMARY (VALID ORDERS)" in report_text
    assert "Reconciliation Check:    PASSED" in report_text
    assert "DB Integrity Check:      PASSED" in report_text


# Scenario 27: Verification of performance report contents
def test_scenario_27_performance_report_contents(tmp_path):
    csv_file = tmp_path / "orders.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD2101,CUST0001,2024-05-01,120.00,completed\n"
    )

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    file_handle = open(str(perf_txt), "r", encoding="utf-8")
    perf_text = file_handle.read()
    file_handle.close()

    assert "E-COMMERCE DATA PIPELINE PERFORMANCE REPORT" in perf_text
    assert "STAGE DURATION BREAKDOWN" in perf_text
    assert "Ingestion & Read:" in perf_text
    assert "Validation & Cleanse:" in perf_text
    assert "DOCUMENTED PERFORMANCE OPTIMIZATION" in perf_text


# Scenario 28: Verification of rejected record traceability
def test_scenario_28_rejected_record_traceability(tmp_path):
    csv_file = tmp_path / "trace_orders.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD2201,CUST9999,2024-05-01,50.00,completed\n"
    )

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    metrics = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    rej_df = pd.read_csv(str(rejected_csv))
    assert len(rej_df) == 1
    row = rej_df.iloc[0]
    assert row["order_id"] == "ORD2201"
    assert row["customer_id"] == "CUST9999"
    assert row["source_file"] == "trace_orders.csv"
    assert "not found in reference list" in row["rejection_reason"]
    assert row["run_id"] == metrics["run_id"]


# Scenario 29: Verification that empty input generates all required reports
def test_scenario_29_empty_input_generates_all_reports(tmp_path):
    empty_csv = tmp_path / "zero_bytes.csv"
    empty_csv.write_text("")

    db_file = tmp_path / "test_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    metrics = run_pipeline(
        input_path=str(empty_csv),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
    )

    assert metrics["execution_status"] == "SUCCESS_EMPTY_FILE"
    assert os.path.exists(str(clean_csv))
    assert os.path.exists(str(rejected_csv))
    assert os.path.exists(str(quality_json))
    assert os.path.exists(str(recon_txt))
    assert os.path.exists(str(perf_txt))
    assert os.path.exists(str(log_file))


# Scenario 30: Practical 100,000-record performance test
def test_scenario_30_performance_100k_records(tmp_path):
    from generate_data import generate_orders_dataset

    large_csv = tmp_path / "large_orders.csv"
    generate_orders_dataset(output_path=str(large_csv), total_rows=100000, random_seed=42)

    db_file = tmp_path / "large_orders.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    metrics = run_pipeline(
        input_path=str(large_csv),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
    )

    assert metrics["execution_status"] == "SUCCESS"
    assert metrics["total_records_read"] == 100000
    assert metrics["valid_records_count"] > 80000
    assert metrics["rejected_records_count"] > 10000
    assert metrics["inserted_records_count"] == metrics["valid_records_count"]
    assert metrics["reconciliation"]["row_reconciliation_passed"] is True
    assert metrics["reconciliation"]["db_reconciliation_passed"] is True
    assert metrics["execution_duration_seconds"] < 15.0


# Scenario 31: Foreign key enforcement and customer reference integrity
def test_scenario_31_foreign_key_enforcement(tmp_path):
    csv_file = tmp_path / "orders.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD3101,CUST0001,2024-05-01,50.00,completed\n"
        "ORD3102,CUST9999,2024-05-02,75.00,completed\n"
    )

    db_file = tmp_path / "test_fk.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    metrics = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    assert metrics["valid_records_count"] == 1
    assert metrics["rejected_records_count"] == 1
    assert metrics["inserted_records_count"] == 1

    conn = sqlite3.connect(str(db_file))
    conn.execute("PRAGMA foreign_keys = ON;")
    cursor = conn.cursor()

    # Check customers table exists and contains CUST0001
    cursor.execute("SELECT customer_id FROM customers WHERE customer_id = 'CUST0001'")
    assert cursor.fetchone() is not None

    # Check orders foreign key constraint via PRAGMA foreign_key_list
    cursor.execute("PRAGMA foreign_key_list(orders)")
    fk_list = cursor.fetchall()
    assert len(fk_list) > 0
    assert any(fk[2] == "customers" and fk[3] == "customer_id" for fk in fk_list)

    # Attempt direct insert with unknown customer_id when PRAGMA foreign_keys = ON
    import pytest
    with pytest.raises(sqlite3.IntegrityError):
        cursor.execute(
            "INSERT INTO orders VALUES ('ORD9999', 'UNKNOWN_CUST', '2024-05-01', 10.0, 'completed', 'test.csv', 'RUN_TEST', '2024-05-01 10:00:00')"
        )
        conn.commit()

    conn.close()


# Scenario 32: Persistent pipeline run history
def test_scenario_32_pipeline_run_history_persistence(tmp_path):
    csv_file_1 = tmp_path / "batch_1.csv"
    csv_file_1.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD3201,CUST0001,2024-05-01,10.00,completed\n"
    )

    db_file = tmp_path / "test_history.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    # First run
    run_pipeline(
        input_path=str(csv_file_1),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    # Second run
    csv_file_2 = tmp_path / "batch_2.csv"
    csv_file_2.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD3202,CUST0001,2024-05-02,20.00,completed\n"
    )

    run_pipeline(
        input_path=str(csv_file_2),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    # Third run with failure
    bad_csv = tmp_path / "bad.csv"
    bad_csv.write_text("order_id,order_date\nORD3203,2024-05-03\n")

    run_pipeline(
        input_path=str(bad_csv),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    # Inspect pipeline_runs table in SQLite
    conn = sqlite3.connect(str(db_file))
    cursor = conn.cursor()
    cursor.execute(
        "SELECT run_id, source_file, status, input_count, valid_count, inserted_count, failure_reason "
        "FROM pipeline_runs ORDER BY start_time ASC"
    )
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 3
    assert rows[0][1] == "batch_1.csv"
    assert rows[0][2] == "SUCCESS"
    assert rows[0][5] == 1

    assert rows[1][1] == "batch_2.csv"
    assert rows[1][2] == "SUCCESS"
    assert rows[1][5] == 1

    assert rows[2][1] == "bad.csv"
    assert rows[2][2] == "FAILED"
    assert "Missing required columns" in rows[2][6]


# Scenario 33: Malformed CSV row quarantine with source row number
def test_scenario_33_malformed_csv_row_quarantine_with_row_number(tmp_path):
    csv_file = tmp_path / "malformed_rows.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD3301,CUST0001,2024-05-01,10.00,completed\n"
        "ORD3302_BAD,CUST0001,2024-05-02,20.00,completed,EXTRA1,EXTRA2\n"
        "ORD3303,CUST0001,2024-05-03,30.00,completed\n"
        "ORD3304_FEW,CUST0001\n"
    )

    db_file = tmp_path / "test_malformed.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    metrics = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    assert metrics["execution_status"] == "SUCCESS"
    assert metrics["total_records_read"] == 4
    assert metrics["malformed_records_count"] == 2
    assert metrics["valid_records_count"] == 2
    assert metrics["rejected_records_count"] == 2
    assert metrics["inserted_records_count"] == 2

    # Verify reconciliation formula accounts for malformed rows
    assert metrics["reconciliation"]["row_reconciliation_passed"] is True

    rej_df = pd.read_csv(str(rejected_csv))
    assert len(rej_df) == 2
    assert 3 in rej_df["source_row_number"].values
    assert 5 in rej_df["source_row_number"].values
    for reason in rej_df["rejection_reason"]:
        assert "Malformed CSV record: expected 5 columns" in reason


# Scenario 34: Conflicting duplicate records vs exact duplicate records
def test_scenario_34_conflicting_duplicate_detection(tmp_path):
    csv_file_1 = tmp_path / "initial.csv"
    csv_file_1.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD3401,CUST0001,2024-05-01,100.00,completed\n"
    )

    db_file = tmp_path / "test_dup_conflict.db"
    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    # Initial run
    run_pipeline(
        input_path=str(csv_file_1),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001", "CUST0002"],
    )

    # Next run contains ORD3401 with modified amount ($999.00 instead of $100.00)
    csv_file_2 = tmp_path / "conflicting.csv"
    csv_file_2.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD3401,CUST0001,2024-05-01,999.00,completed\n"
        "ORD3402,CUST0001,2024-05-02,50.00,completed\n"
    )

    metrics_2 = run_pipeline(
        input_path=str(csv_file_2),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001", "CUST0002"],
    )

    assert metrics_2["inserted_records_count"] == 1
    assert metrics_2["conflicting_duplicate_count"] == 1

    # Verify original order in SQLite was preserved and not overwritten
    conn = sqlite3.connect(str(db_file))
    cursor = conn.cursor()
    cursor.execute("SELECT amount FROM orders WHERE order_id = 'ORD3401'")
    amount_in_db = cursor.fetchone()[0]
    conn.close()
    assert amount_in_db == 100.00


# Scenario 35: Transaction rollback on database error and recovery
def test_scenario_35_transaction_rollback_on_publishing_failure(tmp_path):
    csv_file = tmp_path / "rollback_test.csv"
    csv_file.write_text(
        "order_id,customer_id,order_date,amount,order_status\n"
        "ORD3501,CUST0001,2024-05-01,10.00,completed\n"
        "ORD3502,CUST0001,2024-05-02,20.00,completed\n"
    )

    db_dir_as_file = tmp_path / "directory_target"
    os.makedirs(db_dir_as_file, exist_ok=True)

    clean_csv = tmp_path / "clean_orders.csv"
    rejected_csv = tmp_path / "rejected_orders.csv"
    quality_json = tmp_path / "quality_report.json"
    recon_txt = tmp_path / "reconciliation_report.txt"
    perf_txt = tmp_path / "performance_report.txt"
    log_file = tmp_path / "test.log"

    # Run with invalid DB path to simulate failure
    metrics_fail = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(db_dir_as_file),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    assert metrics_fail["execution_status"] == "FAILED_DATABASE_ERROR"
    assert metrics_fail["inserted_records_count"] == 0

    # Retry with valid database path
    valid_db = tmp_path / "recovered.db"
    metrics_success = run_pipeline(
        input_path=str(csv_file),
        processed_path=str(clean_csv),
        rejected_path=str(rejected_csv),
        db_path=str(valid_db),
        quality_report_path=str(quality_json),
        reconciliation_report_path=str(recon_txt),
        performance_report_path=str(perf_txt),
        log_file_path=str(log_file),
        known_customers=["CUST0001"],
    )

    assert metrics_success["execution_status"] == "SUCCESS"
    assert metrics_success["inserted_records_count"] == 2


