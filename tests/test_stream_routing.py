"""Unit tests for AMQP 0-9-1 topic routing implemented in stream.topic_routing."""

from __future__ import annotations

import pytest

from stream.topic_routing import DRIVER_BINDINGS, matches_topic_pattern, route_key_to_drivers


@pytest.mark.parametrize(
    ("routing_key", "expected_drivers"),
    [
        ("bangalore.koramangala.food", ["Control", "Meena", "Ravi"]),
        ("bangalore.indiranagar.ride", ["Arjun", "Control", "Priya"]),
        ("bangalore.whitefield.ride", ["Arjun", "Control"]),
        ("chennai.tnagar.food", ["Karthik"]),
        ("chennai.whitefield.ride", []),
        ("bangalore.koramangala", ["Control"]),
    ],
)
def test_part_d1_driver_routing(routing_key: str, expected_drivers: list[str]) -> None:
    matched = sorted(route_key_to_drivers(routing_key, DRIVER_BINDINGS))
    assert matched == expected_drivers


def test_wildcard_hash_zero_and_multi_word_matching() -> None:
    assert matches_topic_pattern("#", "bangalore.koramangala.food")
    assert matches_topic_pattern("bangalore.#", "bangalore")
    assert not matches_topic_pattern("bangalore.*", "bangalore")
    assert matches_topic_pattern("#.food", "chennai.mylapore.food")
