"""Exclusive (self) time per service for one trace.

A span's exclusive time is the length of its own ``[start_ns, end_ns)``
interval minus the union of the intersections with its *direct* children's
intervals (children are matched by ``parent_span_id`` inside the same trace).
Self-referencing spans (``parent_span_id == span_id``) are ignored, child
intervals that overlap each other are subtracted only once, and zero-length
spans contribute zero. Pure functions only — no I/O, no sampling policy.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping


def exclusive_by_service(
    rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Aggregate exclusive nanoseconds per service over one trace's spans.

    ``rows`` are mappings with at least ``span_id``, ``parent_span_id``,
    ``service``, ``start_ns`` and ``end_ns`` keys (``sqlite3.Row`` works).
    Returns a list of ``{"service", "span_count", "exclusive_ns"}`` sorted by
    exclusive ns descending, then service name ascending with the null
    (missing-service) group last.
    """
    spans = [
        (
            r["span_id"],
            r["parent_span_id"],
            r["service"],
            int(r["start_ns"]),
            int(r["end_ns"]),
        )
        for r in rows
    ]
    known_ids = {s[0] for s in spans}
    children: dict[str, list[tuple[int, int]]] = {}
    for span_id, parent_id, _service, start_ns, end_ns in spans:
        if parent_id is None or parent_id == span_id or parent_id not in known_ids:
            continue
        children.setdefault(parent_id, []).append((start_ns, end_ns))

    totals: dict[str | None, list[int]] = {}
    for span_id, _parent_id, service, start_ns, end_ns in spans:
        length = max(0, end_ns - start_ns)
        overlaps = []
        for c_start, c_end in children.get(span_id, ()):
            lo = max(c_start, start_ns)
            hi = min(c_end, end_ns)
            if hi > lo:
                overlaps.append((lo, hi))
        exclusive = length - _union_length(overlaps)
        entry = totals.setdefault(service, [0, 0])
        entry[0] += 1
        entry[1] += exclusive

    result = [
        {"service": service, "span_count": count, "exclusive_ns": ns}
        for service, (count, ns) in totals.items()
    ]
    result.sort(key=lambda e: (-e["exclusive_ns"], e["service"] is None,
                               e["service"] or ""))
    return result


def _union_length(intervals: list[tuple[int, int]]) -> int:
    """Total length covered by ``[start, end)`` intervals, overlaps counted once."""
    total = 0
    cur_lo = cur_hi = None
    for lo, hi in sorted(intervals):
        if cur_lo is None:
            cur_lo, cur_hi = lo, hi
        elif lo <= cur_hi:
            cur_hi = max(cur_hi, hi)
        else:
            total += cur_hi - cur_lo
            cur_lo, cur_hi = lo, hi
    if cur_lo is not None:
        total += cur_hi - cur_lo
    return total
