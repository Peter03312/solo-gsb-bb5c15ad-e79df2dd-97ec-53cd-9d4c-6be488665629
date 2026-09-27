"""Time helpers.

Timestamps on the wire are ISO-8601 strings (e.g. ``2026-09-18T10:00:00Z``)
or epoch values in one of three units; everything is normalized internally to
integer nanoseconds since the Unix epoch.
"""
from __future__ import annotations

from datetime import datetime, timezone
import time

# Detection thresholds for numeric epoch values.
_NS_MAX = 10**14        # year ~5138 in nanoseconds
_US_MAX = 10**17        # year ~5138 in microseconds
# anything larger-looking and below _US_MAX is milliseconds; seconds below
# _NS_MAX are ambiguous with milliseconds, but modern ms epoch values are
# > 1e12 while seconds are ~1.7e9, so 1e11 is a safe boundary.
_MS_MIN = 10**11


def now_ns() -> int:
    return time.time_ns()


def to_ns(value: float | int | str | datetime) -> int:
    """Normalize a timestamp to epoch nanoseconds."""
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
    elif isinstance(value, (int, float)):
        return _numeric_to_ns(value)
    else:
        raise ValueError(f"unsupported timestamp type: {type(value)!r}")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000_000)


def _numeric_to_ns(v: float | int) -> int:
    # Integers stay in exact integer arithmetic: epoch ns values exceed 2**53
    # and would be rounded if routed through float.
    a = abs(v)
    if a >= _US_MAX:
        return int(v)                                  # already nanoseconds
    if a >= _NS_MAX:
        return int(v * 1_000)                          # microseconds
    if a >= _MS_MIN:
        return int(v * 1_000_000)                      # milliseconds
    return int(v * 1_000_000_000)                      # seconds


def ns_to_iso(ns: int) -> str:
    dt = datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def minute_bucket(ns: int) -> int:
    """Return the epoch-seconds value of the minute containing ``ns``."""
    return (ns // 60_000_000_000) * 60


def minute_bucket_iso(ns: int) -> str:
    return ns_to_iso(minute_bucket(ns) * 1_000_000_000)
