"""AMQP 0-9-1 topic exchange routing pattern evaluation."""

from __future__ import annotations

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
    p_tokens = pattern.split(".")
    k_tokens = routing_key.split(".")
    p_len, k_len = len(p_tokens), len(k_tokens)

    dp = [[False] * (k_len + 1) for _ in range(p_len + 1)]
    dp[0][0] = True

    for i in range(1, p_len + 1):
        if p_tokens[i - 1] == "#":
            dp[i][0] = dp[i - 1][0]

    for i in range(1, p_len + 1):
        token = p_tokens[i - 1]
        for j in range(1, k_len + 1):
            if token == "#":
                dp[i][j] = dp[i - 1][j] or dp[i][j - 1]
            elif token == "*" or token == k_tokens[j - 1]:
                dp[i][j] = dp[i - 1][j - 1]

    return dp[p_len][k_len]


def route_key_to_drivers(
    routing_key: str,
    bindings: dict[str, str] | None = None,
) -> list[str]:
    """Return all driver names whose binding pattern matches `routing_key`."""
    active_bindings = bindings if bindings is not None else DRIVER_BINDINGS
    return [
        driver
        for driver, pattern in active_bindings.items()
        if matches_topic_pattern(pattern, routing_key)
    ]
