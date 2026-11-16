"""Shared RabbitMQ / CloudAMQP HTTP Management API client."""

from __future__ import annotations

import base64
import json
import urllib.parse
import urllib.request
from typing import Any

DEFAULT_EXCHANGE = "rapido-dispatch"
DEFAULT_EXCHANGE_TYPE = "topic"


class RabbitHttpBroker:
    """Lightweight RabbitMQ client operating over the HTTP(S) Management API."""

    def __init__(self, amqp_url: str, exchange: str = DEFAULT_EXCHANGE) -> None:
        self.amqp_url = amqp_url
        self.exchange = exchange

        parsed = urllib.parse.urlparse(amqp_url)
        self.scheme = parsed.scheme or "amqp"
        self.vhost = urllib.parse.quote(parsed.path.lstrip("/"), safe="") or "%2F"
        self.username = parsed.username or "guest"
        self.password = parsed.password or "guest"
        self.hostname = parsed.hostname or "localhost"
        self.port = parsed.port or (443 if self.scheme == "amqps" else 15672)

        credentials = f"{self.username}:{self.password}".encode("utf-8")
        token = base64.b64encode(credentials).decode("ascii")
        self.headers = {
            "Authorization": f"Basic {token}",
            "Content-Type": "application/json",
        }
        self.api_root = (
            f"https://{self.hostname}/api"
            if self.scheme == "amqps"
            else f"http://{self.hostname}:{self.port}/api"
        )

    def _request(self, method: str, path: str, body: dict[str, Any] | None = None, timeout: int = 10):
        encoded = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(
            f"{self.api_root}{path}",
            data=encoded,
            headers=self.headers,
            method=method,
        )
        return urllib.request.urlopen(req, timeout=timeout)

    def declare_topic_exchange(self, durable: bool = True) -> bool:
        """Create or verify the target topic exchange."""
        with self._request(
            "PUT",
            f"/exchanges/{self.vhost}/{self.exchange}",
            {"type": DEFAULT_EXCHANGE_TYPE, "durable": durable},
        ) as resp:
            return resp.status in (200, 201, 204)

    def publish_json(self, routing_key: str, payload: dict[str, Any]) -> bool:
        """Publish a persistent JSON message to the topic exchange."""
        envelope = {
            "routing_key": routing_key,
            "payload": json.dumps(payload),
            "payload_encoding": "string",
            "properties": {"delivery_mode": 2},
        }
        with self._request(
            "POST",
            f"/exchanges/{self.vhost}/{self.exchange}/publish",
            envelope,
        ) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            return bool(result.get("routed", True))

    def bind_transient_queue(self, queue_name: str, binding_key: str) -> None:
        """Declare a temporary auto-delete queue and bind it to the exchange."""
        self._request(
            "PUT",
            f"/queues/{self.vhost}/{queue_name}",
            {"durable": False, "auto_delete": True},
        ).close()
        self._request(
            "POST",
            f"/bindings/{self.vhost}/e/{self.exchange}/q/{queue_name}",
            {"routing_key": binding_key},
        ).close()

    def delete_queue(self, queue_name: str) -> None:
        """Remove a transient queue if it still exists."""
        try:
            self._request("DELETE", f"/queues/{self.vhost}/{queue_name}", timeout=5).close()
        except Exception:
            pass

    def poll_queue(self, queue_name: str, count: int = 10) -> list[dict[str, Any]]:
        """Fetch up to `count` queued messages via ack_requeue_false."""
        try:
            with self._request(
                "POST",
                f"/queues/{self.vhost}/{queue_name}/get",
                {"count": count, "ackmode": "ack_requeue_false", "encoding": "auto"},
            ) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception:
            return []
