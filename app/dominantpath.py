"""Dominant (heaviest) root-to-leaf call chain for one trace.

Each span's weight is its exclusive time: the length of its own
``[start_ns, end_ns)`` interval minus the union of the intersections with
its *direct* children's intervals (children are matched by
``parent_span_id`` inside the same trace). Child intervals that overlap
each other or stick out of the parent are subtracted only once and only
for the part inside the parent interval, so a weight is never negative.

Spans whose parent never arrived are treated as roots. A snapshot holding
self-referencing spans or parent-reference cycles has no well-defined
root-to-leaf paths; :func:`dominant_path` then raises
:class:`CyclicTraceError` listing the implicated span ids. Pure functions
only — no I/O, no sampling policy.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping

from .integrity import _canonical_cycle, _find_cycles
from .servicetime import _union_length


class CyclicTraceError(Exception):
    """Self references / parent cycles make root-to-leaf paths undefined.

    Carries the implicated span ids in deterministic order:
    ``self_references`` holds the spans that name themselves as parent,
    ``cycles`` holds each parent-reference ring (rotated to start at its
    smallest span id, rings sorted), and ``span_ids`` is the sorted union
    of both.
    """

    def __init__(
        self, self_references: list[str], cycles: list[list[str]]
    ) -> None:
        self.self_references = sorted(self_references)
        self.cycles = sorted(_canonical_cycle(list(ring)) for ring in cycles)
        involved = set(self.self_references)
        for ring in self.cycles:
            involved.update(ring)
        self.span_ids = sorted(involved)
        parts = []
        if self.self_references:
            parts.append(
                "self-referencing span(s): " + ", ".join(self.self_references)
            )
        for ring in self.cycles:
            parts.append("parent cycle: " + " -> ".join(ring + [ring[0]]))
        super().__init__(
            "trace has no root-to-leaf call chain (" + "; ".join(parts) + ")"
        )


def dominant_path(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Heaviest root-to-leaf chain over one trace's spans.

    ``rows`` are mappings with at least ``span_id``, ``parent_span_id``,
    ``start_ns`` and ``end_ns`` keys (``sqlite3.Row`` works). Returns
    ``{"path", "weights_ns", "total_weight_ns"}`` where ``path`` holds the
    span ids root-first and ``weights_ns`` the per-node exclusive weights
    aligned with it. Chains tied on total weight are broken by the
    lexicographically smallest span-id sequence. Raises
    :class:`CyclicTraceError` for self references or parent cycles.
    """
    data = [
        (r["span_id"], r["parent_span_id"],
         int(r["start_ns"]), int(r["end_ns"]))
        for r in rows
    ]
    if not data:
        return {"path": [], "weights_ns": [], "total_weight_ns": 0}
    known_ids = {sid for sid, _pid, _start, _end in data}

    # child -> parent, kept only for parents that arrived; self references
    # and rings are structural errors, missing parents make roots.
    parent_of: dict[str, str] = {}
    self_refs: list[str] = []
    for sid, pid, _start, _end in data:
        if pid is None:
            continue
        if pid == sid:
            self_refs.append(sid)
        elif pid in known_ids:
            parent_of[sid] = pid

    cycles = _find_cycles(parent_of)
    if self_refs or cycles:
        raise CyclicTraceError(self_refs, cycles)

    children: dict[str, list[str]] = {}
    for child, parent in parent_of.items():
        children.setdefault(parent, []).append(child)

    intervals = {sid: (start, end) for sid, _pid, start, end in data}
    weights = {
        sid: _exclusive_weight(
            start, end, (intervals[c] for c in children.get(sid, ()))
        )
        for sid, (start, end) in intervals.items()
    }

    roots = sorted(
        sid for sid, pid, _start, _end in data
        if pid is None or pid not in known_ids
    )

    # The graph is a forest now (one parent per span, no cycles), so a
    # reverse pre-order pass solves every subtree bottom-up.
    order: list[str] = []
    stack = list(roots)
    while stack:
        sid = stack.pop()
        order.append(sid)
        stack.extend(children.get(sid, ()))

    best_total: dict[str, int] = {}
    best_path: dict[str, list[str]] = {}
    for sid in reversed(order):
        total = weights[sid]
        path = [sid]
        choice: tuple[int, list[str]] | None = None
        for c in children.get(sid, ()):
            candidate = (best_total[c], best_path[c])
            if choice is None or (-candidate[0], candidate[1]) < (
                -choice[0], choice[1]
            ):
                choice = candidate
        if choice is not None:
            total += choice[0]
            path += choice[1]
        best_total[sid] = total
        best_path[sid] = path

    top = min(roots, key=lambda r: (-best_total[r], best_path[r]))
    path = best_path[top]
    return {
        "path": path,
        "weights_ns": [weights[sid] for sid in path],
        "total_weight_ns": best_total[top],
    }


def _exclusive_weight(
    start_ns: int, end_ns: int, child_intervals: Iterable[tuple[int, int]]
) -> int:
    """Own interval length minus the union of clipped child intersections."""
    length = max(0, end_ns - start_ns)
    overlaps = []
    for c_start, c_end in child_intervals:
        lo = max(c_start, start_ns)
        hi = min(c_end, end_ns)
        if hi > lo:
            overlaps.append((lo, hi))
    return length - _union_length(overlaps)
