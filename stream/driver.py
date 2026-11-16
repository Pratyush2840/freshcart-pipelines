"""
stream/driver.py
----------------
Part D2 & D3 topic-exchange driver consumer with schema validation and ACK/NACK handling.

Usage:
  python stream/driver.py Ravi "bangalore.koramangala.*"
  python stream/driver.py Arjun "bangalore.*.ride"
  python stream/driver.py Control "bangalore.#"
"""

from __future__ import annotations

import json
import sys
import time
import uuid
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.settings import get_amqp_url
from stream.broker_client import DEFAULT_EXCHANGE, RabbitHttpBroker

EXCHANGE = DEFAULT_EXCHANGE
AMQP_URL = get_amqp_url()


class DriverWorker:
    """Manages a driver's transient queue binding and polling loop."""

    def __init__(self, amqp_url: str, driver_name: str, binding_pattern: str) -> None:
        self.driver_name = driver_name
        self.binding_pattern = binding_pattern
        self.queue_name = f"driver_{driver_name}_{uuid.uuid4().hex[:6]}"
        self.broker = RabbitHttpBroker(amqp_url, exchange=EXCHANGE)

    def start(self) -> None:
        self.broker.bind_transient_queue(self.queue_name, self.binding_pattern)
        print(
            f"[{self.driver_name}] Queue '{self.queue_name}' bound to '{EXCHANGE}' "
            f"with pattern '{self.binding_pattern}'"
        )

    def stop(self) -> None:
        self.broker.delete_queue(self.queue_name)

    def poll(self, batch_size: int = 5) -> list[dict]:
        return self.broker.poll_queue(self.queue_name, count=batch_size)


def process_message(driver_name: str, raw_msg: dict) -> None:
    """Validate incoming message structure and emit ACK or NACK log output."""
    payload_text = raw_msg.get("payload", "")
    routing_key = raw_msg.get("routing_key", "")

    try:
        event = json.loads(payload_text)
    except Exception:
        print(f"[{driver_name}] ✗ INVALID JSON -> NACK (requeue=False, discarded)", flush=True)
        return

    if not event.get("zone"):
        short_id = str(event.get("id", "?"))[:8]
        print(
            f"[{driver_name}] ✗ MALFORMED MESSAGE (missing 'zone' field) -> "
            f"NACK (requeue=False, discarded) | id={short_id}",
            flush=True,
        )
        return

    print(
        f"[{driver_name}] ✓ ACK | key={routing_key:<32} "
        f"city={event['city']} zone={event['zone']} type={event['type']} id={event['id'][:8]}",
        flush=True,
    )


def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: python driver.py <NAME> <PATTERN>")
        print('  e.g. python driver.py Ravi "bangalore.koramangala.*"')
        sys.exit(1)

    driver_name, pattern = sys.argv[1], sys.argv[2]
    worker = DriverWorker(AMQP_URL, driver_name, pattern)
    worker.start()
    print(f"[{driver_name}] Listening for matching orders. Press Ctrl+C to stop.\n")

    try:
        while True:
            for item in worker.poll():
                process_message(driver_name, item)
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n[{driver_name}] Shutting down...")
    finally:
        worker.stop()
        print(f"[{driver_name}] Stopped and queue cleaned up.")


if __name__ == "__main__":
    main()
