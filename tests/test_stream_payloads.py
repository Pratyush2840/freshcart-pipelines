"""Unit tests for stream message construction and environment configuration."""

from __future__ import annotations

from common.settings import get_amqp_url, get_snowflake_params
from stream.dispatcher import build_message


def test_dispatcher_valid_and_poison_pill_cadence() -> None:
    routing_key_ok, valid_msg = build_message(1)
    assert len(routing_key_ok.split(".")) == 3
    assert valid_msg["zone"]
    assert valid_msg["order_id"] == "ORD-STRM-0001"
    assert "_bad" not in valid_msg

    routing_key_bad, bad_msg = build_message(10)
    assert len(routing_key_bad.split(".")) == 3
    assert "zone" not in bad_msg
    assert bad_msg.get("_bad") is True


def test_settings_defaults_are_well_formed() -> None:
    amqp_url = get_amqp_url()
    assert amqp_url.startswith(("amqp://", "amqps://"))

    sf = get_snowflake_params()
    assert set(sf.keys()) == {"account", "user", "password", "role", "warehouse", "database"}
