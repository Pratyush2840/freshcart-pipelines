"""Centralized environment loader and configuration helpers."""

from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
LANDING_ROOT: Path = PROJECT_ROOT / "freshcart_landing"
DEFAULT_AMQP_URL: str = "amqp://guest:guest@localhost:5672/"


def load_env_mapping(env_path: Path | None = None) -> dict[str, str]:
    """Read key=value pairs from `.env` without overriding existing OS env vars."""
    target = env_path or (PROJECT_ROOT / ".env")
    if not target.is_file():
        return {}

    parsed: dict[str, str] = {}
    with target.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            stripped = raw_line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue

            key, val = stripped.split("=", 1)
            cleaned_key = key.strip()
            cleaned_val = val.strip().strip('"').strip("'")
            if cleaned_key:
                parsed[cleaned_key] = cleaned_val
                os.environ.setdefault(cleaned_key, cleaned_val)
    return parsed


def _resolve_setting(name: str, default: str | None = None) -> str | None:
    file_vars = load_env_mapping()
    if file_vars.get(name):
        return file_vars[name]

    env_val = os.environ.get(name)
    return env_val if env_val else default


def get_amqp_url() -> str:
    """Return the configured RabbitMQ/CloudAMQP connection URL."""
    resolved = _resolve_setting("AMQP_URL", DEFAULT_AMQP_URL)
    return resolved or DEFAULT_AMQP_URL


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
