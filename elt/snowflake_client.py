"""Snowflake connection, NDJSON staging, and query helpers for FreshCart ELT."""

from __future__ import annotations

import gzip
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import snowflake.connector

from common.settings import LANDING_ROOT, get_snowflake_params

LANDING_ROOT.mkdir(parents=True, exist_ok=True)


def open_snowflake_connection():
    """Create a Snowflake connection using centralized environment settings."""
    params = get_snowflake_params()
    return snowflake.connector.connect(**params)


def stage_ndjson_batch(source: str, records: list[dict[str, Any]], run_id: str) -> Path:
    """Serialize source records into a gzipped NDJSON envelope file on disk."""
    target_dir = LANDING_ROOT / source
    target_dir.mkdir(parents=True, exist_ok=True)
    output_path = target_dir / f"{source}__{run_id}.json.gz"
    extracted_at = datetime.now(timezone.utc).isoformat()

    with gzip.open(output_path, "wt", encoding="utf-8") as gz_file:
        for item in records:
            envelope = {
                "source": source,
                "run_id": run_id,
                "extracted_at": extracted_at,
                "data": item,
            }
            gz_file.write(json.dumps(envelope, ensure_ascii=False) + "\n")

    print(f"   {source}: {len(records)} records -> {output_path}")
    return output_path


def copy_staged_to_bronze(cur, local_path: Path | str, table: str, stage_dir: str) -> int:
    """Upload a local NDJSON.gz file to @bronze.landing and COPY INTO the Bronze table."""
    file_uri = "file://" + Path(local_path).resolve().as_posix()
    cur.execute(f"PUT '{file_uri}' @bronze.landing/{stage_dir}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
    cur.execute(
        f"""
        COPY INTO {table} (source, raw, run_id, extracted_at, src_file, src_row)
        FROM (
          SELECT
            $1:source::STRING,
            $1:data,
            $1:run_id::STRING,
            $1:extracted_at::TIMESTAMP_TZ,
            METADATA$FILENAME,
            METADATA$FILE_ROW_NUMBER
          FROM @bronze.landing/{stage_dir}/
        )
        FILE_FORMAT = (FORMAT_NAME = 'bronze.ff_ndjson')
        ON_ERROR = 'ABORT_STATEMENT'
        """
    )
    copy_results = cur.fetchall()
    rows_loaded = sum(int(row[3]) for row in copy_results if len(row) > 3)
    print(f"   {rows_loaded} rows -> {table}")
    return rows_loaded


def execute_sql_batch(conn, sql_script: str) -> None:
    """Execute a multi-statement SQL string against Snowflake."""
    conn.execute_string(sql_script, remove_comments=True)


def fetch_rows(cur, sql: str) -> list[tuple]:
    """Execute a SQL query and return all fetched tuples."""
    cur.execute(sql)
    return cur.fetchall()


def fetch_dataframe(cur, sql: str) -> pd.DataFrame:
    """Execute a SQL query and materialize results as a pandas DataFrame."""
    cur.execute(sql)
    columns = [col[0] for col in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=columns)


def use_freshcart_context(cur) -> None:
    """Ensure Snowflake session uses the configured FreshCart warehouse and database."""
    params = get_snowflake_params()
    warehouse = params.get("warehouse") or "FRESHCART_WH"
    database = params.get("database") or "FRESHCART"
    cur.execute(f"USE WAREHOUSE {warehouse}")
    cur.execute(f"USE DATABASE {database}")


# Backwards-compatible aliases
get_connection = open_snowflake_connection
write_landing = stage_ndjson_batch
load_file = copy_staged_to_bronze
run_sql = execute_sql_batch
query = fetch_rows
show = fetch_dataframe
