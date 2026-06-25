"""
etl_runner.py

Core ETL orchestration engine.

Coordinates the complete workflow:

SFTP
    ↓
Validation
    ↓
Checkpoint Recovery
    ↓
Staging
    ↓
MERGE
    ↓
Cleanup
"""

import csv
import time
from pathlib import Path

from common.checkpoint import read_checkpoint, write_checkpoint, clear_checkpoint
from common.database import (
    get_sql_connection,
    get_table_columns,
    get_table_count,
    drop_table_if_exists,
    create_staging_table,
    insert_rows,
)
from common.merge import merge_staging_to_destination
from common.sftp import download_file_from_sftp
from common.validation import validate_columns


DELIMITER = "|"
ENCODING = "utf-8-sig"
BATCH_SIZE = 100000


def create_staging_table_name(job_name):
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    safe_job_name = job_name.replace(" ", "_").replace("-", "_")
    return f"dbo.stg_{safe_job_name}_{timestamp}"


def normalize_value(value):
    if value is None:
        return None

    value = str(value).strip()

    if value in ["", "NULL", "null", "None", "NONE", "nan", "NaN"]:
        return None

    return value


def get_source_columns(file_path):
    with open(file_path, "r", encoding=ENCODING, newline="") as file:
        reader = csv.reader(file, delimiter=DELIMITER)
        columns = next(reader)

    return [column.strip().replace("\ufeff", "") for column in columns]


def read_source_rows(file_path):
    with open(file_path, "r", encoding=ENCODING, newline="") as file:
        reader = csv.DictReader(file, delimiter=DELIMITER)

        for row in reader:
            cleaned_row = {}

            for key, value in row.items():
                if key is None:
                    continue

                clean_key = key.strip().replace("\ufeff", "")
                cleaned_row[clean_key] = normalize_value(value)

            yield cleaned_row


def load_file_to_staging(
    conn,
    job_name,
    file_path,
    staging_table,
    columns_to_load,
    batch_size=BATCH_SIZE,
):
    checkpoint = read_checkpoint(job_name)
    already_loaded = int(checkpoint.get("rows_loaded_to_staging", 0) or 0)

    if already_loaded:
        print(f"[{job_name}] Resuming after {already_loaded:,} staged rows.")

    print(f"[{job_name}] Loading rows to staging...")

    batch = []
    total_loaded_this_run = 0
    total_seen = 0

    for row_number, row in enumerate(read_source_rows(file_path), start=1):
        total_seen = row_number

        if row_number <= already_loaded:
            continue

        filtered_row = {
            column: row.get(column)
            for column in columns_to_load
        }

        batch.append(filtered_row)

        if len(batch) >= batch_size:
            insert_rows(conn, staging_table, columns_to_load, batch)

            total_loaded_this_run += len(batch)
            staged_total = already_loaded + total_loaded_this_run

            write_checkpoint(
                job_name,
                stage="loading_staging",
                staging_table=staging_table,
                rows_loaded_to_staging=staged_total,
            )

            print(f"[{job_name}] Staged {staged_total:,} rows...")
            batch.clear()

    if batch:
        insert_rows(conn, staging_table, columns_to_load, batch)

        total_loaded_this_run += len(batch)
        staged_total = already_loaded + total_loaded_this_run

        write_checkpoint(
            job_name,
            stage="loading_staging",
            staging_table=staging_table,
            rows_loaded_to_staging=staged_total,
        )

        print(f"[{job_name}] Staged {staged_total:,} rows...")
        batch.clear()

    final_total = already_loaded + total_loaded_this_run

    print(f"[{job_name}] Source rows read: {total_seen:,}")
    print(f"[{job_name}] Rows staged: {final_total:,}")

    return final_total


def run_etl(
    job_name,
    source_file,
    destination_table,
    primary_keys,
    batch_size=BATCH_SIZE,
):
    start_time = time.time()
    staging_table = None
    conn = None

    print("=" * 90)
    print(f"Starting ETL Job: {job_name}")
    print(f"Source File: {source_file}")
    print(f"Destination Table: {destination_table}")
    print(f"Primary Key(s): {', '.join(primary_keys)}")
    print("=" * 90)

    try:
        file_path = download_file_from_sftp(
            job_name=job_name,
            source_file=source_file,
        )

        source_columns = get_source_columns(file_path)

        conn = get_sql_connection()

        destination_columns = get_table_columns(conn, destination_table)

        columns_to_load = validate_columns(
            job_name=job_name,
            source_columns=source_columns,
            destination_columns=destination_columns,
            primary_keys=primary_keys,
        )

        staging_table = create_staging_table_name(job_name)

        drop_table_if_exists(conn, staging_table)
        create_staging_table(conn, staging_table, columns_to_load)

        write_checkpoint(
            job_name,
            stage="staging_created",
            staging_table=staging_table,
            rows_loaded_to_staging=0,
        )

        load_file_to_staging(
            conn=conn,
            job_name=job_name,
            file_path=file_path,
            staging_table=staging_table,
            columns_to_load=columns_to_load,
            batch_size=batch_size,
        )

        staging_count = get_table_count(conn, staging_table)
        print(f"[{job_name}] Final staging row count: {staging_count:,}")

        merge_staging_to_destination(
            conn=conn,
            job_name=job_name,
            destination_table=destination_table,
            staging_table=staging_table,
            columns=columns_to_load,
            primary_keys=primary_keys,
        )

        print(f"[{job_name}] Dropping staging table...")
        drop_table_if_exists(conn, staging_table)

        clear_checkpoint(job_name)

        elapsed = (time.time() - start_time) / 60

        print("=" * 90)
        print(f"[{job_name}] ETL COMPLETE")
        print(f"[{job_name}] Elapsed time: {elapsed:.2f} minutes")
        print("=" * 90)

    except Exception as error:
        print("=" * 90)
        print(f"[{job_name}] ETL FAILED")
        print(error)
        print("=" * 90)

        write_checkpoint(
            job_name,
            stage="failed",
            staging_table=staging_table,
            error=str(error),
        )

        raise

    finally:
        if conn is not None:
            conn.close()