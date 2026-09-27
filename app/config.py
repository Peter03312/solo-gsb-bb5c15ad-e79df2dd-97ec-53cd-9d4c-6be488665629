"""Application configuration.

Every value can be overridden through an environment variable so the same
image works in docker-compose, tests and bare-metal deployments.
"""
from __future__ import annotations

import os


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else default


class Settings:
    # SQLite database location (mounted as a volume in docker-compose).
    database_path: str = os.environ.get("TS_DATABASE_PATH", "/data/traces.db")

    # A trace becomes "complete" once no new span has arrived for this many
    # seconds (the silence / idle window).
    idle_window_seconds: float = _env_float("TS_IDLE_WINDOW_SECONDS", 10.0)

    # Background aggregator tick frequency.
    tick_interval_seconds: float = _env_float("TS_TICK_INTERVAL_SECONDS", 1.0)

    # Traces whose end-to-end duration (ns) exceeds this are classified slow.
    slow_threshold_ns: int = _env_int(
        "TS_SLOW_THRESHOLD_NS", 1_000_000_000  # 1 second
    )

    # Total retained traces allowed per wall-clock minute. Error/slow traces
    # are evaluated first against this budget; normal traffic fills whatever
    # remains via stable-hash sampling.
    sample_budget_per_minute: int = _env_int("TS_SAMPLE_BUDGET_PER_MINUTE", 100)


settings = Settings()
