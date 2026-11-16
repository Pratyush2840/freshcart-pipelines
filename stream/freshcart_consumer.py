"""
stream/freshcart_consumer.py
----------------------------
Lakehouse bridge consuming '#.food' events from RabbitMQ over a 60s micro-batch window,
validating schema (ACK/NACK), staging via write_landing(), and running Snowflake ELT.
"""

from __future__ import annotations

import json
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.settings import LANDING_ROOT, get_amqp_url
from stream.broker_client import DEFAULT_EXCHANGE, DEFAULT_EXCHANGE_TYPE, RabbitHttpBroker

try:
    from elt.pipeline_runner import (
        copy_staged_to_bronze,
        execute_elt_transformations,
        open_snowflake_connection,
        run_quality_checks,
        stage_ndjson_batch,
        use_freshcart_context,
    )
except ImportError:
    pass

LANDING_ROOT.mkdir(parents=True, exist_ok=True)

EXCHANGE = DEFAULT_EXCHANGE
EXCHANGE_TYPE = DEFAULT_EXCHANGE_TYPE
BINDING_PATTERN = "#.food"
LISTEN_SECONDS = 60
AMQP_URL = get_amqp_url()


class StreamConsumer:
    """Micro-batch RabbitMQ consumer writing food orders to the FreshCart landing zone."""

    def __init__(self, url: str, pattern: str = BINDING_PATTERN) -> None:
        self.url = url
        self.pattern = pattern
        self.queue_name = f"freshcart_lakehouse_{uuid.uuid4().hex[:6]}"
        self.broker = RabbitHttpBroker(url, exchange=EXCHANGE)

    def setup(self) -> None:
        self.broker.bind_transient_queue(self.queue_name, self.pattern)
        print(f"[consumer] Bound queue '{self.queue_name}' to '{EXCHANGE}' with key '{self.pattern}'")

    def cleanup(self) -> None:
        self.broker.delete_queue(self.queue_name)

    def fetch_batch(self, count: int = 20) -> list[dict[str, Any]]:
        return self.broker.poll_queue(self.queue_name, count=count)


def _to_landing_order(msg: dict[str, Any], index: int) -> dict[str, Any]:
    return {
        "order_id": msg.get("order_id", f"ORD-STRM-{index:04d}"),
        "cust_name": msg.get("cust_name", "Priya Raman"),
        "cust_email": msg.get("cust_email", "priya.r@gmail.com"),
        "order_date": msg.get("order_date", "2026-09-10"),
        "amount": str(msg.get("amount", "1200.00")),
        "currency": msg.get("currency", "INR"),
        "pincode": msg.get("pincode", "560034"),
        "updated_at": msg.get("updated_at", datetime.now(timezone.utc).isoformat()),
    }


def collect_stream_orders(
    consumer: StreamConsumer | None = None,
    duration_seconds: int = LISTEN_SECONDS,
) -> list[dict[str, Any]]:
    """Buffer valid food-order messages from RabbitMQ for `duration_seconds`."""
    managed = consumer is None
    active_consumer = consumer or StreamConsumer(AMQP_URL, pattern=BINDING_PATTERN)
    if managed:
        active_consumer.setup()

    print(f"\n[consumer] Starting 1-minute batch collection window ({duration_seconds}s)...")
    print(f"[consumer] Listening to exchange '{EXCHANGE}' with binding '{BINDING_PATTERN}'\n")

    buffered: list[dict[str, Any]] = []
    rejected_count = 0
    started_at = time.time()
    deadline = started_at + duration_seconds

    try:
        while time.time() < deadline:
            remaining = int(deadline - time.time())
            for item in active_consumer.fetch_batch(count=10):
                raw_payload = item.get("payload", "{}")
                routing_key = item.get("routing_key", "")
                try:
                    msg = json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload
                except Exception:
                    continue

                if not msg.get("zone"):
                    rejected_count += 1
                    print(
                        f"  [NACK - DISCARDED] Malformed order id={str(msg.get('id', '?'))[:8]} missing 'zone'"
                    )
                    continue

                record = _to_landing_order(msg, len(buffered) + 1)
                buffered.append(record)
                print(
                    f"  [ACK - ACCEPTED]  Key={routing_key:<24} "
                    f"Order={record['order_id']} Cust={record['cust_email']} "
                    f"Amt=INR {record['amount']}"
                )

            time.sleep(2)
            if remaining > 0 and remaining % 15 == 0:
                print(f"[consumer] {remaining}s remaining... ({len(buffered)} orders buffered)")
    finally:
        if managed:
            active_consumer.cleanup()

    elapsed = int(time.time() - started_at)
    print(f"\n[consumer] Finished {elapsed}s window.")
    print(f"[consumer] Valid orders accepted: {len(buffered)}")
    print(f"[consumer] Malformed orders rejected (NACKed): {rejected_count}")
    return buffered


def run_pipeline_with_stream(records: list[dict[str, Any]], run_id: str):
    """Persist buffered stream orders to landing storage and run Snowflake ELT."""
    if not records:
        print("[consumer] No valid records to process.")
        return None

    print("\n--- FreshCart Landing Stage ---")
    landing_path = stage_ndjson_batch(source="web", records=records, run_id=run_id)

    print("\n--- Loading into Snowflake Bronze ---")
    conn = open_snowflake_connection()
    try:
        with conn.cursor() as cur:
            use_freshcart_context(cur)
            copy_staged_to_bronze(cur, landing_path, "bronze.orders_raw", f"stream_{run_id}")

            print("\n--- Executing Transformation SQL (Silver & Gold) ---")
            execute_elt_transformations(
                conn,
                cur,
                run_id=run_id,
                include_unsupported_currency=True,
                resolve_missing_pincodes=False,
            )

            print("\n--- Running Quality Checks ---")
            run_quality_checks(cur, include_sixth_check=True)

            print("\n--- Current Gold Orders Table ---")
            cur.execute(
                """
                SELECT order_id, cust_id, email, tier, multiplier, amount_inr, loyalty_points, published_at
                FROM gold.fct_orders
                ORDER BY order_id
                """
            )
            gold_rows = cur.fetchall()
            print(f"{'ORDER_ID':<16} {'CUST_ID':<10} {'TIER':<10} {'AMOUNT_INR':<12} {'POINTS':<8} {'PUBLISHED_AT'}")
            print("-" * 75)
            for row in gold_rows:
                print(f"{row[0]:<16} {row[1]:<10} {row[3]:<10} {str(row[5]):<12} {row[6]:<8} {str(row[7])[:19]}")

            cur.execute("SELECT COUNT(*), SUM(amount_inr), SUM(loyalty_points) FROM gold.fct_orders")
            totals = cur.fetchone()
            print("-" * 75)
            print(f"Total Gold Orders: {totals[0]} | Total Revenue: INR {totals[1]} | Total Points: {totals[2]}")
            return totals
    finally:
        conn.close()


if __name__ == "__main__":
    collected = collect_stream_orders(duration_seconds=LISTEN_SECONDS)
    run_pipeline_with_stream(collected, f"stream_{int(time.time())}")
