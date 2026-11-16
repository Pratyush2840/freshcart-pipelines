"""AMQP 0-9-1 topic exchange routing pattern evaluation."""

from __future__ import annotations

from collections.abc import Mapping

DRIVER_BINDINGS: dict[str, str] = {
    "Ravi": "bangalore.koramangala.*",
    "Priya": "bangalore.indiranagar.*",
    "Arjun": "bangalore.*.ride",
    "Meena": "bangalore.*.food",
    "Karthik": "chennai.tnagar.food",
    "Control": "bangalore.#",
}


def matches_topic_pattern(pattern: str, routing_key: str) -> bool:
    """
    Evaluate AMQP 0-9-1 topic matching using iterative dynamic programming.

    Rules:
      - '*' matches exactly one dot-delimited word.
      - '#' matches zero or more dot-delimited words.
    """
    pattern_tokens = pattern.split(".")
    key_tokens = routing_key.split(".")
    p_count, k_count = len(pattern_tokens), len(key_tokens)

    table: list[list[bool]] = [[False] * (k_count + 1) for _ in range(p_count + 1)]
    table[0][0] = True

    for row in range(1, p_count + 1):
        if pattern_tokens[row - 1] == "#":
            table[row][0] = table[row - 1][0]

    for row in range(1, p_count + 1):
        token = pattern_tokens[row - 1]
        for col in range(1, k_count + 1):
            if token == "#":
                table[row][col] = table[row - 1][col] or table[row][col - 1]
            elif token == "*" or token == key_tokens[col - 1]:
                table[row][col] = table[row - 1][col - 1]

    return table[p_count][k_count]


def route_key_to_drivers(
    routing_key: str,
    bindings: Mapping[str, str] | None = None,
) -> list[str]:
    """Return all driver names whose binding pattern matches `routing_key`."""
    active_bindings = bindings if bindings is not None else DRIVER_BINDINGS
    return [
        driver
        for driver, pattern in active_bindings.items()
        if matches_topic_pattern(pattern, routing_key)
    ]
