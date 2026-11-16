"""
stream/dispatcher.py
--------------------
Part D2 & D3 event producer publishing FreshCart/Rapido dispatch orders every 2 seconds
to the 'rapido-dispatch' RabbitMQ topic exchange. Every 10th event omits 'zone' to test NACKs.
"""

from __future__ import annotations

import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.settings import get_amqp_url
from stream.broker_client import DEFAULT_EXCHANGE, DEFAULT_EXCHANGE_TYPE, RabbitHttpBroker

EXCHANGE = DEFAULT_EXCHANGE
EXCHANGE_TYPE = DEFAULT_EXCHANGE_TYPE
AMQP_URL = get_amqp_url()

CITIES = ("bangalore", "chennai")
CITY_ZONES: dict[str, tuple[str, ...]] = {
    "bangalore": ("koramangala", "indiranagar", "whitefield"),
    "chennai": ("tnagar", "mylapore", "anna_nagar"),
}
ORDER_TYPES = ("ride", "food")

CUSTOMER_POOL: tuple[tuple[str, str], ...] = (
    ("Priya Raman", "priya.r@gmail.com"),
    ("Arjun Kumar", "arjun.k@yahoo.in"),
    ("Meera Patel", "meera@outlook.com"),
)
CITY_PINCODES: dict[str, str] = {
    "bangalore": "560034",
    "chennai": "600041",
}
AMOUNT_PRESETS: tuple[float, ...] = (950.00, 1400.00, 2200.00, 3100.00, 1800.00)


def build_message(seq: int) -> tuple[str, dict]:
    """Construct a (routing_key, payload) pair; inject malformed event every 10th seq."""
    city = random.choice(CITIES)
    zone = random.choice(CITY_ZONES[city])
    order_type = random.choice(ORDER_TYPES)
    routing_key = f"{city}.{zone}.{order_type}"

    event_id = str(uuid.uuid4())
    now_ts = time.time()

    if seq % 10 == 0:
        return routing_key, {
            "id": event_id,
            "city": city,
            "type": order_type,
            "ts": now_ts,
            "_bad": True,
        }

    cust_name, cust_email = CUSTOMER_POOL[(seq - 1) % len(CUSTOMER_POOL)]
    amount = AMOUNT_PRESETS[(seq - 1) % len(AMOUNT_PRESETS)]
    return routing_key, {
        "id": event_id,
        "city": city,
        "zone": zone,
        "type": order_type,
        "ts": now_ts,
        "order_id": f"ORD-STRM-{seq:04d}",
        "cust_name": cust_name,
        "cust_email": cust_email,
        "order_date": "2026-09-10",
        "amount": amount,
        "currency": "INR",
        "pincode": CITY_PINCODES.get(city, "560034"),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    broker_host = AMQP_URL.split("@")[-1] if "@" in AMQP_URL else "localhost"
    print(f"[dispatcher] Connecting to RabbitMQ ({broker_host})...")
    broker = RabbitHttpBroker(AMQP_URL, exchange=EXCHANGE)
    broker.declare_topic_exchange(durable=True)
    print(f"[dispatcher] Declared topic exchange '{EXCHANGE}'.")
    print("[dispatcher] Publishing orders every 2 seconds. Press Ctrl+C to stop.\n")

    seq = 0
    try:
        while True:
            seq += 1
            routing_key, payload = build_message(seq)
            broker.publish_json(routing_key, payload)
            suffix = " *** MALFORMED (no zone) -> Expected NACK ***" if payload.get("_bad") else ""
            print(f"[{seq:04d}] Published key={routing_key:<32} id={payload['id'][:8]}{suffix}", flush=True)
            time.sleep(2)
    except KeyboardInterrupt:
        print("\n[dispatcher] Stopped.", flush=True)


if __name__ == "__main__":
    main()
