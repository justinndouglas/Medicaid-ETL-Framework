"""
ESDOutpatient Full ETL Loader
Recreated production-style script

Purpose:
    Download ESDOutpatient.txt from SFTP and load it into Azure SQL table dbo.ESDOutpatient.

Workflow:
    1. Connect to SFTP
    2. Download ESDOutpatient.txt with visible progress
    3. Resume partial downloads if the .part file exists
    4. Read pipe-delimited file with headers
    5. Create a SQL staging table
    6. Batch insert rows into staging
    7. MERGE staging into dbo.ESDOutpatient using ESDOutpatientID
    8. Prevent duplicate key rows in staging
    9. Save checkpoints during load
    10. Drop staging table after successful merge

Install:
    pip install paramiko pyodbc python-dotenv

Required .env:
    SFTP_HOST=
    SFTP_PORT=22
    SFTP_USERNAME=
    SFTP_PASSWORD=
    SFTP_REMOTE_DIR=

    AZURE_SQL_SERVER=
    AZURE_SQL_DATABASE=NACC DataBase
    AZURE_SQL_USERNAME=
    AZURE_SQL_PASSWORD=
    ODBC_DRIVER=ODBC Driver 18 for SQL Server
"""

from __future__ import annotations

import csv
import json
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

import paramiko
import pyodbc
from dotenv import load_dotenv


# =============================================================================
# USER SETTINGS
# =============================================================================

JOB_NAME = "ESDOutpatient"
SOURCE_FILE = "ESDOutpatient.txt"
DESTINATION_TABLE = "dbo.ESDOutpatient"
PRIMARY_KEYS = ["ESDOutpatientID"]

DELIMITER = "|"
ENCODING = "utf-8-sig"
BATCH_SIZE = 100000

DOWNLOAD_DIR = Path("downloads")
CHECKPOINT_DIR = Path("checkpoints")

NULL_VALUES = {"", "NULL", "null", "None", "NONE", "nan", "NaN"}


# =============================================================================
# ENVIRONMENT / CONNECTIONS
# =============================================================================

def load_settings() -> None:
    load_dotenv()


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required .env value: {name}")
    return value


def get_sql_connection() -> pyodbc.Connection:
    load_settings()

    server = get_required_env("AZURE_SQL_SERVER")
    database = get_required_env("AZURE_SQL_DATABASE")
    username = get_required_env("AZURE_SQL_USERNAME")
    password = get_required_env("AZURE_SQL_PASSWORD")
    driver = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")

    connection_string = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=60;"
    )

    return pyodbc.connect(connection_string, autocommit=False)


def get_sftp_client() -> paramiko.SFTPClient:
    load_settings()

    host = get_required_env("SFTP_HOST")
    port = int(os.getenv("SFTP_PORT", "22"))
    username = get_required_env("SFTP_USERNAME")
    password = get_required_env("SFTP_PASSWORD")
    remote_dir = os.getenv("SFTP_REMOTE_DIR", ".")

    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)

    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp.chdir(remote_dir)

    return sftp


# =============================================================================
# CHECKPOINTS
# =============================================================================

def ensure_folders() -> None:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)


def checkpoint_file() -> Path:
    return CHECKPOINT_DIR / f"{JOB_NAME}.json"


def read_checkpoint() -> Dict:
    path = checkpoint_file()
    if not path.exists():
        return {
            "job_name": JOB_NAME,
            "stage": "new",
            "rows_loaded_to_staging": 0,
            "staging_table": None,
        }

    try:
        return json.loads(path.read_text())
    except Exception:
        return {
            "job_name": JOB_NAME,
            "stage": "new",
            "rows_loaded_to_staging": 0,
            "staging_table": None,
        }


def write_checkpoint(**updates) -> None:
    data = read_checkpoint()
    data.update(updates)
    data["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    checkpoint_file().write_text(json.dumps(data, indent=2))


def clear_checkpoint() -> None:
    path = checkpoint_file()
    if path.exists():
        path.unlink()


# =============================================================================
# DISPLAY / PROGRESS
# =============================================================================

def print_header() -> None:
    print("=" * 90)
    print(f"Starting ETL Job: {JOB_NAME}")
    print(f"Source File:       {SOURCE_FILE}")
    print(f"Destination:       {DESTINATION_TABLE}")
    print(f"Primary Key(s):    {', '.join(PRIMARY_KEYS)}")
    print(f"Batch Size:        {BATCH_SIZE:,}")
    print("=" * 90)


def progress_bar(current: int, total: int, label: str = "Progress") -> None:
    if total <= 0:
        print(f"\r{label}: {current:,} bytes", end="")
        return

    pct = min(current / total, 1)
    width = 35
    filled = int(width * pct)
    bar = "█" * filled + "-" * (width - filled)

    print(
        f"\r{label}: |{bar}| {pct:7.2%} "
        f"({current:,}/{total:,} bytes)",
        end="",
        flush=True,
    )


def elapsed_minutes(start_time: float) -> float:
    return (time.time() - start_time) / 60


# =============================================================================
# FILE DOWNLOAD
# =============================================================================

def download_file_from_sftp() -> Path:
    ensure_folders()

    local_file = DOWNLOAD_DIR / SOURCE_FILE
    partial_file = DOWNLOAD_DIR / f"{SOURCE_FILE}.part"

    print(f"\n[{JOB_NAME}] Connecting to SFTP...")

    sftp = get_sftp_client()

    try:
        remote_size = sftp.stat(SOURCE_FILE).st_size

        if local_file.exists() and local_file.stat().st_size == remote_size:
            print(f"[{JOB_NAME}] Existing complete download found: {local_file}")
            write_checkpoint(stage="downloaded", local_file=str(local_file))
            return local_file

        already_downloaded = partial_file.stat().st_size if partial_file.exists() else 0

        mode = "ab" if already_downloaded > 0 else "wb"

        print(f"[{JOB_NAME}] Downloading {SOURCE_FILE}")
        if already_downloaded:
            print(f"[{JOB_NAME}] Resuming download from {already_downloaded:,} bytes")

        with sftp.open(SOURCE_FILE, "rb") as remote, open(partial_file, mode) as local:
            if already_downloaded:
                remote.seek(already_downloaded)

            downloaded = already_downloaded

            while True:
                chunk = remote.read(1024 * 1024)
                if not chunk:
                    break

                local.write(chunk)
                downloaded += len(chunk)
                progress_bar(downloaded, remote_size, label="Downloading")

        print()

        shutil.move(str(partial_file), str(local_file))

        print(f"[{JOB_NAME}] Download complete: {local_file}")
        write_checkpoint(stage="downloaded", local_file=str(local_file))

        return local_file

    finally:
        try:
            sftp.close()
        except Exception:
            pass


# =============================================================================
# FILE READING / CLEANING
# =============================================================================

def normalize_value(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None

    value = str(value).strip()

    if value in NULL_VALUES:
        return None

    return value


def get_source_columns(file_path: Path) -> List[str]:
    with open(file_path, "r", encoding=ENCODING, newline="") as f:
        reader = csv.reader(f, delimiter=DELIMITER)
        columns = next(reader)

    cleaned = [c.strip().replace("\ufeff", "") for c in columns]

    if not cleaned:
        raise RuntimeError("No columns found in source file.")

    return cleaned


def iter_source_rows(file_path: Path) -> Iterable[Dict[str, Optional[str]]]:
    with open(file_path, "r", encoding=ENCODING, newline="") as f:
        reader = csv.DictReader(f, delimiter=DELIMITER)

        for row in reader:
            cleaned = {}
            for key, value in row.items():
                if key is None:
                    continue
                cleaned_key = key.strip().replace("\ufeff", "")
                cleaned[cleaned_key] = normalize_value(value)
            yield cleaned


# =============================================================================
# SQL HELPERS
# =============================================================================

def split_table_name(table_name: str) -> tuple[str, str]:
    if "." in table_name:
        schema, table = table_name.split(".", 1)
    else:
        schema, table = "dbo", table_name

    return schema.replace("[", "").replace("]", ""), table.replace("[", "").replace("]", "")


def bracket_table(table_name: str) -> str:
    schema, table = split_table_name(table_name)
    return f"[{schema}].[{table}]"


def bracket_column(column_name: str) -> str:
    return f"[{column_name}]"


def object_exists_sql(table_name: str) -> str:
    schema, table = split_table_name(table_name)
    return f"OBJECT_ID(N'[{schema}].[{table}]', N'U')"


def create_staging_table_name() -> str:
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    return f"dbo.stg_{JOB_NAME}_{timestamp}"


def drop_table_if_exists(conn: pyodbc.Connection, table_name: str) -> None:
    sql = f"""
    IF {object_exists_sql(table_name)} IS NOT NULL
    BEGIN
        DROP TABLE {bracket_table(table_name)};
    END
    """

    with conn.cursor() as cur:
        cur.execute(sql)

    conn.commit()


def create_staging_table(conn: pyodbc.Connection, staging_table: str, columns: Sequence[str]) -> None:
    column_definitions = ",\n    ".join(
        f"{bracket_column(column)} NVARCHAR(MAX) NULL"
        for column in columns
    )

    sql = f"""
    CREATE TABLE {bracket_table(staging_table)}
    (
        {column_definitions}
    );
    """

    print(f"[{JOB_NAME}] Creating staging table: {staging_table}")

    with conn.cursor() as cur:
        cur.execute(sql)

    conn.commit()


def insert_rows_to_staging(
    conn: pyodbc.Connection,
    staging_table: str,
    columns: Sequence[str],
    rows: List[Dict[str, Optional[str]]],
) -> None:
    if not rows:
        return

    column_sql = ", ".join(bracket_column(c) for c in columns)
    placeholders = ", ".join("?" for _ in columns)

    sql = f"""
    INSERT INTO {bracket_table(staging_table)}
    ({column_sql})
    VALUES ({placeholders});
    """

    values = [
        [row.get(column) for column in columns]
        for row in rows
    ]

    with conn.cursor() as cur:
        cur.fast_executemany = True
        cur.executemany(sql, values)

    conn.commit()


def get_count(conn: pyodbc.Connection, table_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {bracket_table(table_name)};")
        row = cur.fetchone()

    return int(row[0])


# =============================================================================
# DESTINATION VALIDATION
# =============================================================================

def get_destination_columns(conn: pyodbc.Connection, destination_table: str) -> List[str]:
    schema, table = split_table_name(destination_table)

    sql = """
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ?
      AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION;
    """

    with conn.cursor() as cur:
        cur.execute(sql, schema, table)
        rows = cur.fetchall()

    return [row[0] for row in rows]


def validate_columns(
    source_columns: Sequence[str],
    destination_columns: Sequence[str],
) -> List[str]:
    destination_set = {c.lower(): c for c in destination_columns}

    matched_columns = []

    for source_column in source_columns:
        key = source_column.lower()
        if key in destination_set:
            matched_columns.append(destination_set[key])

    missing_primary_keys = [
        pk for pk in PRIMARY_KEYS
        if pk.lower() not in {c.lower() for c in matched_columns}
    ]

    if missing_primary_keys:
        raise RuntimeError(
            "Primary key columns are missing from matched source/destination columns: "
            + ", ".join(missing_primary_keys)
        )

    if not matched_columns:
        raise RuntimeError("No matching columns found between source file and destination table.")

    ignored = [c for c in source_columns if c.lower() not in destination_set]
    if ignored:
        print(f"[{JOB_NAME}] Warning: source columns not found in destination and will be ignored:")
        for col in ignored:
            print(f"    - {col}")

    return matched_columns


# =============================================================================
# STAGING LOAD
# =============================================================================

def load_file_to_staging(
    conn: pyodbc.Connection,
    file_path: Path,
    staging_table: str,
    columns: Sequence[str],
) -> int:
    checkpoint = read_checkpoint()
    already_loaded = int(checkpoint.get("rows_loaded_to_staging", 0) or 0)

    if already_loaded:
        print(f"[{JOB_NAME}] Resuming staging load after {already_loaded:,} rows.")

    print(f"[{JOB_NAME}] Loading rows to staging in batches of {BATCH_SIZE:,}...")

    batch = []
    total_seen = 0
    total_inserted_this_run = 0

    for row_number, row in enumerate(iter_source_rows(file_path), start=1):
        total_seen = row_number

        if row_number <= already_loaded:
            continue

        filtered_row = {column: row.get(column) for column in columns}
        batch.append(filtered_row)

        if len(batch) >= BATCH_SIZE:
            insert_rows_to_staging(conn, staging_table, columns, batch)
            total_inserted_this_run += len(batch)
            staged_total = already_loaded + total_inserted_this_run

            write_checkpoint(
                stage="loading_staging",
                staging_table=staging_table,
                rows_loaded_to_staging=staged_total,
            )

            print(f"[{JOB_NAME}] Staged {staged_total:,} rows...")
            batch.clear()

    if batch:
        insert_rows_to_staging(conn, staging_table, columns, batch)
        total_inserted_this_run += len(batch)
        staged_total = already_loaded + total_inserted_this_run

        write_checkpoint(
            stage="loading_staging",
            staging_table=staging_table,
            rows_loaded_to_staging=staged_total,
        )

        print(f"[{JOB_NAME}] Staged {staged_total:,} rows...")
        batch.clear()

    final_staged = already_loaded + total_inserted_this_run

    print(f"[{JOB_NAME}] Source rows read: {total_seen:,}")
    print(f"[{JOB_NAME}] Total rows staged: {final_staged:,}")

    return final_staged


# =============================================================================
# MERGE LOGIC
# =============================================================================

def build_merge_sql(
    destination_table: str,
    staging_table: str,
    columns: Sequence[str],
    primary_keys: Sequence[str],
) -> str:
    destination = bracket_table(destination_table)
    staging = bracket_table(staging_table)

    primary_key_set = {pk.lower() for pk in primary_keys}
    update_columns = [c for c in columns if c.lower() not in primary_key_set]

    on_clause = " AND ".join(
        f"T.{bracket_column(pk)} = S.{bracket_column(pk)}"
        for pk in primary_keys
    )

    non_null_pk_filter = " AND ".join(
        f"{bracket_column(pk)} IS NOT NULL"
        for pk in primary_keys
    )

    partition_by = ", ".join(bracket_column(pk) for pk in primary_keys)
    order_by = ", ".join(bracket_column(pk) for pk in primary_keys)

    insert_columns = ", ".join(bracket_column(c) for c in columns)
    insert_values = ", ".join(f"S.{bracket_column(c)}" for c in columns)

    if update_columns:
        update_set = ",\n            ".join(
            f"T.{bracket_column(c)} = S.{bracket_column(c)}"
            for c in update_columns
        )

        matched_clause = f"""
    WHEN MATCHED THEN
        UPDATE SET
            {update_set}
"""
    else:
        matched_clause = ""

    sql = f"""
MERGE {destination} AS T
USING
(
    SELECT {", ".join(bracket_column(c) for c in columns)}
    FROM
    (
        SELECT
            {", ".join(bracket_column(c) for c in columns)},
            ROW_NUMBER() OVER
            (
                PARTITION BY {partition_by}
                ORDER BY {order_by}
            ) AS RowNumberForDeduplication
        FROM {staging}
        WHERE {non_null_pk_filter}
    ) AS Deduped
    WHERE RowNumberForDeduplication = 1
) AS S
ON {on_clause}
{matched_clause}
    WHEN NOT MATCHED BY TARGET THEN
        INSERT ({insert_columns})
        VALUES ({insert_values});
"""

    return sql


def merge_staging_to_destination(
    conn: pyodbc.Connection,
    destination_table: str,
    staging_table: str,
    columns: Sequence[str],
) -> None:
    print(f"[{JOB_NAME}] Starting MERGE into {destination_table}...")

    before_count = get_count(conn, destination_table)
    print(f"[{JOB_NAME}] Destination row count before MERGE: {before_count:,}")

    merge_sql = build_merge_sql(
        destination_table=destination_table,
        staging_table=staging_table,
        columns=columns,
        primary_keys=PRIMARY_KEYS,
    )

    with conn.cursor() as cur:
        cur.execute(merge_sql)

    conn.commit()

    after_count = get_count(conn, destination_table)
    print(f"[{JOB_NAME}] Destination row count after MERGE: {after_count:,}")
    print(f"[{JOB_NAME}] Net new rows added: {after_count - before_count:,}")

    write_checkpoint(stage="merged")


# =============================================================================
# MAIN
# =============================================================================

def run() -> None:
    start_time = time.time()
    ensure_folders()
    print_header()

    staging_table = None
    conn = None

    try:
        # Step 1: Download file
        file_path = download_file_from_sftp()

        # Step 2: Read source header
        source_columns = get_source_columns(file_path)
        print(f"[{JOB_NAME}] Source columns found: {len(source_columns):,}")

        # Step 3: Connect to SQL
        print(f"[{JOB_NAME}] Connecting to Azure SQL...")
        conn = get_sql_connection()

        # Step 4: Validate destination table and matched columns
        destination_columns = get_destination_columns(conn, DESTINATION_TABLE)
        print(f"[{JOB_NAME}] Destination columns found: {len(destination_columns):,}")

        columns_to_load = validate_columns(source_columns, destination_columns)
        print(f"[{JOB_NAME}] Columns selected for load: {len(columns_to_load):,}")

        # Step 5: Create staging
        staging_table = create_staging_table_name()
        drop_table_if_exists(conn, staging_table)
        create_staging_table(conn, staging_table, columns_to_load)

        write_checkpoint(
            stage="staging_created",
            staging_table=staging_table,
            rows_loaded_to_staging=0,
        )

        # Step 6: Load file to staging
        load_file_to_staging(conn, file_path, staging_table, columns_to_load)

        staging_count = get_count(conn, staging_table)
        print(f"[{JOB_NAME}] Final staging row count: {staging_count:,}")

        # Step 7: Merge
        merge_staging_to_destination(
            conn=conn,
            destination_table=DESTINATION_TABLE,
            staging_table=staging_table,
            columns=columns_to_load,
        )

        # Step 8: Cleanup
        print(f"[{JOB_NAME}] Dropping staging table...")
        drop_table_if_exists(conn, staging_table)

        clear_checkpoint()

        print("=" * 90)
        print(f"[{JOB_NAME}] LOAD COMPLETE")
        print(f"[{JOB_NAME}] Total elapsed time: {elapsed_minutes(start_time):.2f} minutes")
        print("=" * 90)

    except Exception as exc:
        print("=" * 90, file=sys.stderr)
        print(f"[{JOB_NAME}] LOAD FAILED", file=sys.stderr)
        print(str(exc), file=sys.stderr)
        print("=" * 90, file=sys.stderr)

        write_checkpoint(
            stage="failed",
            staging_table=staging_table,
            error=str(exc),
        )

        print(
            f"[{JOB_NAME}] Checkpoint saved at {checkpoint_file()}",
            file=sys.stderr,
        )
        print(
            f"[{JOB_NAME}] If staging table was created, it was preserved for troubleshooting: {staging_table}",
            file=sys.stderr,
        )

        raise

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    run()
