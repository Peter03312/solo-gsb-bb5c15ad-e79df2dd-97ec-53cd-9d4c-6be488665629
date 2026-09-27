"""Concurrent-span windows for one trace.

Sweeps the nanosecond ``[start_ns, end_ns)`` intervals of a trace's spans and
reports the maximal time windows where at least ``min_active`` spans were
running at the same time. Intervals are half-open, so a span ending exactly
where another starts never produces a false overlap; zero-length spans are
empty intervals and never count. Pure functions only — no I/O, no sampling
policy.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping


def active_windows(
    rows: Iterable[Mapping[str, Any]], min_active: int = 2
) -> list[dict[str, int]]:
    """Maximal windows where >= ``min_active`` spans are concurrently active.

    ``rows`` are mappings with at least ``start_ns`` and ``end_ns`` keys
    (``sqlite3.Row`` works). Returns a list of
    ``{"start_ns", "end_ns", "duration_ns", "peak_active"}`` sorted by start;
    stretches that stay at or above the threshold across adjacent event
    points merge into one continuous window, and ``peak_active`` is the
    highest simultaneous count reached inside it.
    """
    if min_active < 2:
        raise ValueError("min_active must be >= 2")

    # Net delta of the active-span count at each instant. Ends and starts at
    # the same timestamp cancel inside one event, so half-open semantics hold.
    events: dict[int, int] = {}
    for r in rows:
        start = int(r["start_ns"])
        end = int(r["end_ns"])
        if end <= start:  # zero-length spans are empty: never active
            continue
        events[start] = events.get(start, 0) + 1
        events[end] = events.get(end, 0) - 1

    windows: list[dict[str, int]] = []
    active = 0
    run_start: int | None = None
    peak = 0
    for t in sorted(events):
        # The count after applying the delta at t holds over [t, next point).
        active += events[t]
        if active >= min_active:
            if run_start is None:
                run_start = t
                peak = active
            elif active > peak:
                peak = active
        elif run_start is not None:
            windows.append({
                "start_ns": run_start,
                "end_ns": t,
                "duration_ns": t - run_start,
                "peak_active": peak,
            })
            run_start = None
    # Deltas sum to zero, so the count is 0 after the last event and no run
    # can still be open here.
    return windows
