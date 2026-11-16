"""Centralized environment loader and configuration helpers."""

from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
LANDING_ROOT: Path = PROJECT_ROOT / "freshcart_landing"


def load_env_mapping(env_path: Path | None = None) -> dict[str, str]:
    """Read key=value pairs from `.env` without overriding existing OS env vars."""
    target = env_path or (PROJECT_ROOT / ".env")
    parsed: dict[str, str] = {}
    if not target.is_file():
        return parsed

    with target.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            stripped = raw_line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, val = stripped.split("=", 1)
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key:
                parsed[key] = val
                os.environ.setdefault(key, val)
    return parsed


def _resolve_setting(name: str, default: str | None = None) -> str | None:
    file_vars = load_env_mapping()
    if name in file_vars and file_vars[name] != "":
        return file_vars[name]
    env_val = os.environ.get(name)
    if env_val is not None and env_val != "":
        return env_val
    return default


def get_amqp_url() -> str:
    """Return the configured RabbitMQ/CloudAMQP connection URL."""
    return _resolve_setting("AMQP_URL", "amqp://guest:guest@localhost:5672/") or "amqp://guest:guest@localhost:5672/"


def get_snowflake_params() -> dict[str, str | None]:
    """Return Snowflake connection parameters sourced from `.env` or environment."""
    return {
        "account": _resolve_setting("SNOWFLAKE_ACCOUNT"),
        "user": _resolve_setting("SNOWFLAKE_USER"),
        "password": _resolve_setting("SNOWFLAKE_PASSWORD"),
        "role": _resolve_setting("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "warehouse": _resolve_setting("SNOWFLAKE_WAREHOUSE", "FRESHCART_WH"),
        "database": _resolve_setting("SNOWFLAKE_DATABASE", "FRESHCART"),
    }
