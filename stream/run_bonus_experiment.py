"""
stream/run_bonus_experiment.py
------------------------------
End-to-end Lakehouse streaming and replay idempotency experiment:
  1. Capture baseline Gold metrics.
  2. Stream RabbitMQ '#.food' events for 60s and run ELT (Run 1).
  3. Force-replay the exact same staged file into Bronze and rerun ELT (Run 2).
  4. Verify zero duplicate rows, zero revenue inflation, and zero point drift.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from elt.pipeline_runner import (
    copy_staged_to_bronze,
    execute_elt_transformations,
    open_snowflake_connection,
    run_quality_checks,
    stage_ndjson_batch,
    use_freshcart_context,
)
from stream.freshcart_consumer import AMQP_URL, StreamConsumer, collect_stream_orders

LOG_FILE = PROJECT_ROOT / "stream" / "bonus_experiment.log"


def _log(msg: str, handle) -> None:
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    handle.write(line + "\n")
    handle.flush()


def _snapshot_gold(conn) -> dict[str, object]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*), COALESCE(SUM(amount_inr), 0), COALESCE(SUM(loyalty_points), 0) "
            "FROM gold.fct_orders"
        )
        count, revenue, points = cur.fetchone()
        cur.execute(
            "SELECT order_id, cust_id, tier, multiplier, amount_inr, loyalty_points "
            "FROM gold.fct_orders ORDER BY order_id"
        )
        rows = cur.fetchall()
        return {
            "count": int(count),
            "revenue": float(revenue),
            "points": int(points),
            "orders": rows,
        }


def run_full_bonus_experiment() -> None:
    """Execute the 2-pass stream ingestion and replay idempotency experiment."""
    with LOG_FILE.open("w", encoding="utf-8") as log_f:
        _log("=" * 80, log_f)
        _log("  BONUS EXPERIMENT: CONNECT RABBITMQ TO SNOWFLAKE LAKEHOUSE", log_f)
        _log("=" * 80, log_f)

        conn = open_snowflake_connection()
        try:
            with conn.cursor() as cur:
                use_freshcart_context(cur)

            _log("\n[STEP 1] Recording Baseline Gold Table Metrics...", log_f)
            baseline = _snapshot_gold(conn)
            _log(f"  Existing Gold Rows    : {baseline['count']}", log_f)
            _log(f"  Existing Gold Revenue : INR {baseline['revenue']:,.2f}", log_f)
            _log(f"  Existing Gold Points  : {baseline['points']}", log_f)
            _log("  Existing Orders in Gold:", log_f)
            for row in baseline["orders"]:
                _log(
                    f"    {row[0]}: cust={row[1]} tier={row[2]} mult={row[3]} inr={row[4]} pts={row[5]}",
                    log_f,
                )

            _log("\n[STEP 2] Launching Dispatcher and Consumer for 60s Batch Window...", log_f)
            consumer = StreamConsumer(AMQP_URL, pattern="#.food")
            consumer.setup()
            _log(f"  Consumer queue '{consumer.queue_name}' declared and bound to '#.food'", log_f)

            dispatcher_proc = subprocess.Popen(
                [sys.executable, "-u", str(PROJECT_ROOT / "stream" / "dispatcher.py")],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                cwd=str(PROJECT_ROOT),
            )
            _log(f"  Started Dispatcher (PID {dispatcher_proc.pid}) publishing to 'rapido-dispatch'...", log_f)

            try:
                collected = collect_stream_orders(consumer=consumer, duration_seconds=60)
            finally:
                dispatcher_proc.terminate()
                dispatcher_proc.wait()
                consumer.cleanup()
                _log("  Dispatcher stopped and consumer queue cleaned up.", log_f)

            _log(f"  Successfully collected {len(collected)} valid FreshCart orders from RabbitMQ.", log_f)

            run_id_1 = "bonus_stream_batch_01"
            _log(f"\n[STEP 3] Writing Landing File and Ingesting Batch 1 (run_id={run_id_1})...", log_f)
            landing_path = stage_ndjson_batch("web", collected, run_id_1)
            _log(f"  Landing file generated: {landing_path}", log_f)

            with conn.cursor() as cur:
                copy_staged_to_bronze(cur, landing_path, "bronze.orders_raw", f"batch_{run_id_1}")
                _log("  Executing Silver & Gold Transformation SQL...", log_f)
                execute_elt_transformations(
                    conn,
                    cur,
                    run_id=run_id_1,
                    include_unsupported_currency=True,
                    resolve_missing_pincodes=False,
                )
                run_quality_checks(cur, include_sixth_check=True)

            after_run_1 = _snapshot_gold(conn)
            _log("\n--- Metrics After Run 1 (Stream Ingestion) ---", log_f)
            _log(
                f"  Gold Rows    : {after_run_1['count']} "
                f"(increased by {after_run_1['count'] - baseline['count']})",
                log_f,
            )
            _log(
                f"  Gold Revenue : INR {after_run_1['revenue']:,.2f} "
                f"(+INR {after_run_1['revenue'] - baseline['revenue']:,.2f})",
                log_f,
            )
            _log(
                f"  Gold Points  : {after_run_1['points']} "
                f"(+{after_run_1['points'] - baseline['points']})",
                log_f,
            )
            _log("  All Current Gold Orders:", log_f)
            for row in after_run_1["orders"]:
                _log(f"    {row[0]}: cust={row[1]} tier={row[2]} inr={row[4]} pts={row[5]}", log_f)

            run_id_2 = "bonus_stream_batch_01_REPLAY"
            _log("\n" + "=" * 80, log_f)
            _log("[STEP 4] MANDATORY IDEMPOTENCY EXPERIMENT: REPLAYING SAME BATCH", log_f)
            _log("=" * 80, log_f)
            _log(f"  Replaying raw landing file: {landing_path}", log_f)
            _log("  Forcing raw load into Bronze with identical records...", log_f)

            with conn.cursor() as cur:
                file_uri = "file://" + Path(landing_path).resolve().as_posix()
                cur.execute(
                    f"PUT '{file_uri}' @bronze.landing/replay_stage/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                )
                cur.execute(
                    """
                    COPY INTO bronze.orders_raw (source, raw, run_id, extracted_at, src_file, src_row)
                    FROM (
                      SELECT $1:source::STRING, $1:data, $1:run_id::STRING,
                             $1:extracted_at::TIMESTAMP_TZ, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
                      FROM @bronze.landing/replay_stage/
                    )
                    FILE_FORMAT = (FORMAT_NAME = 'bronze.ff_ndjson')
                    FORCE = TRUE
                    """
                )
                reloaded = cur.fetchall()
                _log(f"  Raw Bronze re-load status: {reloaded[0][1]} ({reloaded[0][3]} records)", log_f)

                _log("  Re-executing complete ELT pipeline (Silver -> Dedup -> SCD2 -> FX -> Gold MERGE)...", log_f)
                execute_elt_transformations(
                    conn,
                    cur,
                    run_id=run_id_2,
                    include_unsupported_currency=True,
                    resolve_missing_pincodes=False,
                )
                run_quality_checks(cur, include_sixth_check=True)

            after_replay = _snapshot_gold(conn)
            _log("\n--- Metrics After Replaying Exact Same Batch (Run 2) ---", log_f)
            _log(f"  Gold Rows    : {after_replay['count']} (Expected: {after_run_1['count']})", log_f)
            _log(
                f"  Gold Revenue : INR {after_replay['revenue']:,.2f} "
                f"(Expected: INR {after_run_1['revenue']:,.2f})",
                log_f,
            )
            _log(f"  Gold Points  : {after_replay['points']} (Expected: {after_run_1['points']})", log_f)

            rows_delta = after_replay["count"] - after_run_1["count"]
            rev_delta = after_replay["revenue"] - after_run_1["revenue"]
            pts_delta = after_replay["points"] - after_run_1["points"]

            _log("\n" + "=" * 80, log_f)
            _log("  IDEMPOTENCY VERIFICATION RESULTS", log_f)
            _log("=" * 80, log_f)
            _log(
                f"  Delta Gold Rows    : {rows_delta}  -> "
                f"{'PASS (No duplicates created)' if rows_delta == 0 else 'FAIL'}",
                log_f,
            )
            _log(
                f"  Delta Gold Revenue : INR {rev_delta:,.2f}  -> "
                f"{'PASS (Revenue did not double)' if rev_delta == 0.0 else 'FAIL'}",
                log_f,
            )
            _log(
                f"  Delta Gold Points  : {pts_delta}  -> "
                f"{'PASS (Points unchanged)' if pts_delta == 0 else 'FAIL'}",
                log_f,
            )
            _log("=" * 80, log_f)
            _log("\nBonus Pipeline Experiment Finished Successfully.", log_f)
        finally:
            conn.close()


if __name__ == "__main__":
    run_full_bonus_experiment()
