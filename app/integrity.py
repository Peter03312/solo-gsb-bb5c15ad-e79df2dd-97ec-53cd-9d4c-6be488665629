"""Structural integrity checks for one trace.

Validates the parent/child references formed by the spans received so far and
the time containment of every child inside its parent:

* ``missing_parent``     - a span names a ``parent_span_id`` that never arrived;
* ``self_reference``     - a span lists itself as its parent;
* ``cycle``              - two or more spans reference each other in a ring;
                           each ring is reported exactly once, with every
                           member in ring order;
* ``time_out_of_bounds`` - a span whose ``[start_ns, end_ns)`` interval is not
                           contained in its parent's interval.

Edges that participate in a cycle are excluded from the time check: following
the ring there is no meaningful containing interval. Intervals are compared
with inclusive bounds, so a child touching the parent's endpoints is fine.
Pure functions only - no I/O, no sampling policy.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping

MISSING_PARENT = "missing_parent"
SELF_REFERENCE = "self_reference"
CYCLE = "cycle"
TIME_OUT_OF_BOUNDS = "time_out_of_bounds"

_TYPE_ORDER = {
    MISSING_PARENT: 0,
    SELF_REFERENCE: 1,
    CYCLE: 2,
    TIME_OUT_OF_BOUNDS: 3,
}


def check_integrity(
    rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Return all reference/time-boundary anomalies for one trace's spans.

    ``rows`` are mappings with at least ``span_id``, ``parent_span_id``,
    ``start_ns`` and ``end_ns`` keys (``sqlite3.Row`` works). The result is
    deterministic: grouped by anomaly type (missing parent, self reference,
    cycle, time out of bounds) and sorted by the spans involved within each
    group. Cycle member lists are rotated to start at their smallest span id.
    """
    data = [
        (r["span_id"], r["parent_span_id"],
         int(r["start_ns"]), int(r["end_ns"]))
        for r in rows
    ]
    intervals = {sid: (start, end) for sid, _pid, start, end in data}

    # child -> parent, kept only when the parent exists and the edge is not a
    # self reference; those two cases are anomalies of their own.
    parent_of: dict[str, str] = {}
    anomalies: list[dict[str, Any]] = []

    for sid, pid, _start, _end in data:
        if pid is None:
            continue
        if pid == sid:
            anomalies.append({
                "type": SELF_REFERENCE,
                "span_ids": [sid],
                "parent_span_id": sid,
                "message": f"span {sid!r} references itself as its parent",
            })
        elif pid not in intervals:
            anomalies.append({
                "type": MISSING_PARENT,
                "span_ids": [sid],
                "parent_span_id": pid,
                "message": (
                    f"span {sid!r} references unknown parent span {pid!r}"
                ),
            })
        else:
            parent_of[sid] = pid

    cycles = _find_cycles(parent_of)
    cycle_edges: set[tuple[str, str]] = set()
    for cycle in cycles:
        canonical = _canonical_cycle(cycle)
        nodes = set(canonical)
        for child in cycle:
            cycle_edges.add((child, parent_of[child]))
        ring = " -> ".join(canonical + [canonical[0]])
        anomalies.append({
            "type": CYCLE,
            "span_ids": canonical,
            "parent_span_id": None,
            "message": (
                f"cycle of {len(canonical)} spans detected: {ring}"
            ),
        })

    for child, parent in parent_of.items():
        if (child, parent) in cycle_edges:
            continue
        c_start, c_end = intervals[child]
        p_start, p_end = intervals[parent]
        early = c_start < p_start
        late = c_end > p_end
        if not (early or late):
            continue
        problem = " and ".join(
            part
            for cond, part in (
                (early, "starts before its parent"),
                (late, "ends after its parent"),
            )
            if cond
        )
        anomalies.append({
            "type": TIME_OUT_OF_BOUNDS,
            "span_ids": [child, parent],
            "parent_span_id": parent,
            "message": f"span {child!r} {problem} span {parent!r}",
            "child_start_ns": c_start,
            "child_end_ns": c_end,
            "parent_start_ns": p_start,
            "parent_end_ns": p_end,
            "start_overflow_ns": max(0, p_start - c_start),
            "end_overflow_ns": max(0, c_end - p_end),
        })

    anomalies.sort(key=lambda a: (_TYPE_ORDER[a["type"]], a["span_ids"]))
    return anomalies


def _find_cycles(parent_of: dict[str, str]) -> list[list[str]]:
    """Return every directed cycle in a child->parent functional graph.

    Each span has at most one outgoing edge, so cycles are node-disjoint. A
    cycle is reported once even if several lead-in chains reach it: a node
    already explored (state 2) never reopens a walk, and a lead-in node hit
    mid-walk (state 1 from the current path) marks the cycle start.
    """
    # 0/unset = unseen, 1 = on the current walk, 2 = fully explored.
    state: dict[str, int] = {}
    cycles: list[list[str]] = []
    for start in parent_of:
        if state.get(start, 0) != 0:
            continue
        path: list[str] = []
        position: dict[str, int] = {}
        node: str | None = start
        while node is not None and state.get(node, 0) == 0:
            state[node] = 1
            position[node] = len(path)
            path.append(node)
            node = parent_of.get(node)
        if node is not None and state.get(node) == 1:
            # A gray node can only belong to this walk; earlier walks leave
            # all their nodes black.
            cycles.append(path[position[node]:])
        for n in path:
            state[n] = 2
    return cycles


def _canonical_cycle(cycle: list[str]) -> list[str]:
    """Rotate a ring so it starts at its smallest span id (order preserved)."""
    pivot = min(range(len(cycle)), key=lambda i: cycle[i])
    return cycle[pivot:] + cycle[:pivot]
