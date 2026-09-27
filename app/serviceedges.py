"""Cross-service direct call edges for one trace.

A parent/child pair counts as a service edge only when the parent span has
arrived, both spans carry a non-empty ``service`` attribute, and the two
services differ; edges are aggregated by ``(source_service, target_service)``
with a call count. The edge duration is computed per parent span and target
service: every qualifying direct child's ``[start_ns, end_ns)`` interval is
intersected with the parent's interval, the intersections are unioned
(overlapping children count once), and the per-parent unions are then summed
across parent spans. Missing parents, self references and zero-length
intersections contribute no duration. Pure functions only — no I/O, no
sampling policy.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping

from .servicetime import _union_length


def service_edges(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate cross-service call edges over one trace's spans.

    ``rows`` are mappings with at least ``span_id``, ``parent_span_id``,
    ``service``, ``start_ns`` and ``end_ns`` keys (``sqlite3.Row`` works).
    Returns a list of ``{"source_service", "target_service", "call_count",
    "duration_ns"}`` sorted by duration ns descending, then source and target
    service name ascending.
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
    by_id = {s[0]: s for s in spans}

    counts: dict[tuple[str, str], int] = {}
    # (parent span id, target service) -> clipped child intersections.
    overlaps: dict[tuple[str, str], list[tuple[int, int]]] = {}
    for span_id, parent_id, service, start_ns, end_ns in spans:
        if parent_id is None or parent_id == span_id:
            continue
        parent = by_id.get(parent_id)
        if parent is None:
            continue  # parent never arrived: no edge
        p_service = parent[2]
        if not p_service or not service or p_service == service:
            continue
        key = (p_service, service)
        counts[key] = counts.get(key, 0) + 1
        lo = max(start_ns, parent[3])
        hi = min(end_ns, parent[4])
        if hi > lo:
            overlaps.setdefault((parent_id, service), []).append((lo, hi))

    durations: dict[tuple[str, str], int] = {}
    for (parent_id, service), intervals in overlaps.items():
        p_service = by_id[parent_id][2]
        key = (p_service, service)
        durations[key] = durations.get(key, 0) + _union_length(intervals)

    result = [
        {
            "source_service": source,
            "target_service": target,
            "call_count": count,
            "duration_ns": durations.get((source, target), 0),
        }
        for (source, target), count in counts.items()
    ]
    result.sort(key=lambda e: (-e["duration_ns"], e["source_service"],
                               e["target_service"]))
    return result
