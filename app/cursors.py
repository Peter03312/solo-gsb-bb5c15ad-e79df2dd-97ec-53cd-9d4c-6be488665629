"""Opaque pagination cursors.

A cursor carries the full ordering key (``start_ns``, ``trace_id``,
``span_id``) of the last returned row **bound to the query filters it was
issued for** (the ``[start, end)`` nanosecond window and the optional
``service``). Replaying a cursor with different filters is rejected so a
page boundary can never silently skip or duplicate rows.
"""
from __future__ import annotations

import base64
import json
from dataclasses import dataclass

_VERSION = 1


class InvalidCursorError(ValueError):
    """Raised when a cursor token is malformed or does not match the query."""


@dataclass(frozen=True)
class ErrorSpanCursor:
    start_ns: int
    trace_id: str
    span_id: str
    range_start_ns: int
    range_end_ns: int
    service: str | None


def encode(cursor: ErrorSpanCursor) -> str:
    payload = {
        "v": _VERSION,
        "start_ns": cursor.start_ns,
        "trace_id": cursor.trace_id,
        "span_id": cursor.span_id,
        "range_start_ns": cursor.range_start_ns,
        "range_end_ns": cursor.range_end_ns,
        "service": cursor.service,
    }
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def decode(token: str) -> ErrorSpanCursor:
    """Decode and validate a cursor token.

    Raises InvalidCursorError on any structural problem; ``check_binding``
    covers filter mismatches.
    """
    try:
        padded = token + "=" * (-len(token) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
        payload = json.loads(raw)
    except (ValueError, TypeError) as exc:
        raise InvalidCursorError("cursor is not valid base64-encoded json") from exc

    if not isinstance(payload, dict) or payload.get("v") != _VERSION:
        raise InvalidCursorError("unsupported cursor version")

    int_fields = ("start_ns", "range_start_ns", "range_end_ns")
    str_fields = ("trace_id", "span_id")
    try:
        values = {k: payload[k] for k in (*int_fields, *str_fields, "service")}
    except KeyError as exc:
        raise InvalidCursorError(f"cursor is missing field {exc.args[0]!r}") from exc

    # bool is a subclass of int: reject it explicitly.
    if any(isinstance(values[k], bool) or not isinstance(values[k], int)
           for k in int_fields):
        raise InvalidCursorError("cursor contains a non-integer timestamp")
    if any(not isinstance(values[k], str) or not values[k] for k in str_fields):
        raise InvalidCursorError("cursor contains an invalid id")
    if values["service"] is not None and (
        not isinstance(values["service"], str) or not values["service"]
    ):
        raise InvalidCursorError("cursor contains an invalid service filter")

    return ErrorSpanCursor(
        start_ns=values["start_ns"],
        trace_id=values["trace_id"],
        span_id=values["span_id"],
        range_start_ns=values["range_start_ns"],
        range_end_ns=values["range_end_ns"],
        service=values["service"],
    )


def check_binding(
    cursor: ErrorSpanCursor,
    *,
    range_start_ns: int,
    range_end_ns: int,
    service: str | None,
) -> None:
    """Ensure the cursor was minted for exactly this time window and service."""
    if (cursor.range_start_ns, cursor.range_end_ns, cursor.service) != (
        range_start_ns,
        range_end_ns,
        service,
    ):
        raise InvalidCursorError(
            "cursor does not match the requested time range or service filter"
        )
