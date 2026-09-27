"""Unit tests for timestamp normalization and stable hashing."""
from __future__ import annotations

import pytest

from app.timeutil import minute_bucket, to_ns
from app.sampler import hash_score, select_by_hash, stable_hash
from app.servicetime import exclusive_by_service, _union_length
from app.concurrency import active_windows
from app.integrity import (
    CYCLE,
    MISSING_PARENT,
    SELF_REFERENCE,
    TIME_OUT_OF_BOUNDS,
    check_integrity,
)
from app.dominantpath import CyclicTraceError, dominant_path
from app.serviceedges import service_edges
from app.models import SpanIn
from pydantic import ValidationError


def _row(span_id, start_ns, end_ns, *, parent=None, service="svc"):
    return {
        "span_id": span_id,
        "parent_span_id": parent,
        "service": service,
        "start_ns": start_ns,
        "end_ns": end_ns,
    }


def _by_service(rows):
    return {e["service"]: e for e in exclusive_by_service(rows)}


def test_union_length_merges_overlapping_and_abutting():
    assert _union_length([]) == 0
    assert _union_length([(10, 40)]) == 30
    assert _union_length([(10, 40), (30, 80)]) == 70
    assert _union_length([(0, 10), (10, 20)]) == 20
    assert _union_length([(0, 10), (20, 30)]) == 20
    # Identical interval counts once.
    assert _union_length([(0, 50), (0, 50)]) == 50


def test_exclusive_time_subtracts_direct_children():
    rows = [
        _row("root", 0, 100, parent=None),
        _row("child", 20, 60, parent="root"),
    ]
    svcs = _by_service(rows)
    assert svcs["svc"]["span_count"] == 2
    # root: 100 - 40 = 60; child: 40 leaf time.
    assert svcs["svc"]["exclusive_ns"] == 100


def test_overlapping_children_are_subtracted_only_once():
    rows = [
        _row("root", 0, 100),
        _row("a", 10, 40, parent="root"),
        _row("b", 30, 80, parent="root"),
    ]
    # union [10,80) = 70 -> root exclusive 30; leaves add 30 + 50 = 80.
    assert _by_service(rows)["svc"]["exclusive_ns"] == 110


def test_child_extending_beyond_parent_is_clipped_to_parent():
    rows = [
        _row("root", 10, 50),
        _row("child", 0, 100, parent="root"),
    ]
    # root exclusive 0; child exclusive 100.
    assert _by_service(rows)["svc"]["exclusive_ns"] == 100


def test_self_referencing_parent_is_ignored():
    rows = [_row("loop", 0, 30, parent="loop")]
    assert _by_service(rows)["svc"]["exclusive_ns"] == 30


def test_missing_parent_is_not_a_child_of_anything():
    # Orphan span pointing at a span that never arrived keeps full time.
    rows = [_row("orphan", 0, 40, parent="absent-parent")]
    assert _by_service(rows)["svc"]["exclusive_ns"] == 40


def test_zero_duration_span_contributes_zero():
    rows = [
        _row("root", 0, 100),
        _row("instant", 50, 50, parent="root"),
    ]
    assert _by_service(rows)["svc"]["exclusive_ns"] == 100


def test_missing_service_lands_in_null_group():
    rows = [
        _row("a", 0, 30, service=None),
        _row("b", 0, 20, service="svc"),
    ]
    entries = {e["service"]: e for e in exclusive_by_service(rows)}
    assert entries[None] == {
        "service": None, "span_count": 1, "exclusive_ns": 30
    }
    assert entries["svc"]["exclusive_ns"] == 20


def test_sorting_ns_desc_name_asc_null_last():
    rows = [
        _row("b1", 0, 20, service="zsvc"),     # exclusive 20
        _row("b2", 0, 20, service="asvc"),     # exclusive 20, name tie-break
        _row("c", 0, 100, service="msvc"),     # exclusive 100
        _row("n", 0, 20, service=None),        # null, last among the 20s
    ]
    result = exclusive_by_service(rows)
    assert [(e["service"], e["exclusive_ns"]) for e in result] == [
        ("msvc", 100), ("asvc", 20), ("zsvc", 20), (None, 20),
    ]


def test_nested_spans_exclusive_sums_to_wall_clock_length():
    rows = [
        _row("root", 0, 100, service="a"),
        _row("mid", 10, 90, parent="root", service="b"),
        _row("leaf", 20, 80, parent="mid", service="c"),
    ]
    # a:20, b:20, c:60 -> total 100, the trace length.
    result = _by_service(rows)
    assert result["a"]["exclusive_ns"] == 20
    assert result["b"]["exclusive_ns"] == 20
    assert result["c"]["exclusive_ns"] == 60


def test_active_windows_basic_overlap_and_peak():
    rows = [
        _row("a", 0, 10),
        _row("b", 2, 8),
        _row("c", 4, 6),
    ]
    # active: 1@0, 2@2, 3@4, 2@6, 1@8, 0@10 -> one window [2, 8), peak 3.
    assert active_windows(rows, 2) == [
        {"start_ns": 2, "end_ns": 8, "duration_ns": 6, "peak_active": 3}
    ]


def test_active_windows_touching_endpoints_do_not_overlap():
    # Half-open [start, end): b starts exactly where a ends.
    rows = [_row("a", 0, 10), _row("b", 10, 20)]
    assert active_windows(rows, 2) == []


def test_active_windows_adjacent_qualifying_stretches_merge():
    # a ends where c starts (no false overlap), but b keeps the count at 2
    # across the seam at t=10, so [5, 15) is one continuous window.
    rows = [
        _row("a", 0, 10),
        _row("b", 5, 15),
        _row("c", 10, 20),
    ]
    assert active_windows(rows, 2) == [
        {"start_ns": 5, "end_ns": 15, "duration_ns": 10, "peak_active": 2}
    ]


def test_active_windows_separate_windows_and_min_active_threshold():
    rows = [
        _row("a", 0, 10),
        _row("b", 2, 4),
        _row("c", 20, 30),
        _row("d", 22, 28),
        _row("e", 24, 26),
    ]
    assert active_windows(rows, 2) == [
        {"start_ns": 2, "end_ns": 4, "duration_ns": 2, "peak_active": 2},
        {"start_ns": 22, "end_ns": 28, "duration_ns": 6, "peak_active": 3},
    ]
    # With a threshold of 3 only the second stretch qualifies.
    assert active_windows(rows, 3) == [
        {"start_ns": 24, "end_ns": 26, "duration_ns": 2, "peak_active": 3}
    ]
    # Nothing ever reaches 4.
    assert active_windows(rows, 4) == []


def test_active_windows_zero_duration_spans_are_ignored():
    rows = [
        _row("a", 0, 10),
        _row("instant", 5, 5),   # empty interval: never active
        _row("b", 5, 5),
    ]
    assert active_windows(rows, 2) == []


def test_active_windows_identical_spans_and_empty_input():
    assert active_windows([], 2) == []
    rows = [_row("a", 0, 10), _row("b", 0, 10)]
    assert active_windows(rows, 2) == [
        {"start_ns": 0, "end_ns": 10, "duration_ns": 10, "peak_active": 2}
    ]


def test_active_windows_rejects_min_active_below_two():
    with pytest.raises(ValueError):
        active_windows([_row("a", 0, 10)], 1)


def test_epoch_units_are_normalized_to_ns():
    assert to_ns(1_700_000_000) == 1_700_000_000 * 10**9
    assert to_ns(1_700_000_000_000) == 1_700_000_000 * 10**9
    assert to_ns(1_700_000_000_000_000) == 1_700_000_000 * 10**9
    assert to_ns(1_700_000_000_000_000_000) == 1_700_000_000 * 10**9
    assert to_ns(1_700_000_000.5) == 1_700_000_000 * 10**9 + 500_000_000


def test_integer_epoch_ns_round_trips_exactly():
    # Values above 2**53 are not all representable as float; integer input
    # must not be routed through float on the way out.
    odd_ns = 1_577_840_400_010_000_001
    assert to_ns(odd_ns) == odd_ns


def test_iso8601_z_and_offsets():
    assert to_ns("2023-11-14T22:13:20Z") == 1_700_000_000 * 10**9
    assert to_ns("2023-11-14T23:13:20+01:00") == 1_700_000_000 * 10**9
    # Naive timestamps are interpreted as UTC.
    assert to_ns("2023-11-14T22:13:20") == 1_700_000_000 * 10**9


@pytest.mark.parametrize("bad", ["not-a-time", "2026-13-99", None])
def test_bad_timestamps_raise(bad):
    with pytest.raises((ValueError, TypeError)):
        to_ns(bad)


def test_minute_bucket_aligns_downward():
    base = 1_700_000_040  # epoch seconds, known multiple of 60
    assert minute_bucket((base + 59) * 10**9) == base
    assert minute_bucket((base + 60) * 10**9) == base + 60


def test_hash_is_stable_and_spread():
    assert stable_hash("abc") == stable_hash("abc")
    assert stable_hash("abc") != stable_hash("abd")
    scores = [hash_score(f"trace-{i}") for i in range(2000)]
    assert min(scores) >= 0 and max(scores) < 1
    # Rough uniformity: roughly half on each side of 0.5.
    below = sum(1 for s in scores if s < 0.5)
    assert 800 < below < 1200


def test_span_invalid_timestamp_is_rejected():
    with pytest.raises(ValidationError):
        SpanIn(trace_id="t", span_id="s", start_time="nope", end_time=1)
    # end before start is schema-valid but rejected per-span by the engine.
    span = SpanIn(trace_id="t", span_id="s", start_time=10, end_time=5)
    assert span.trace_id == "t"


def test_select_by_hash_exact_counts_and_stability():
    ids = [f"t{i}" for i in range(20)]
    kept1, dropped1 = select_by_hash(ids, 7)
    kept2, dropped2 = select_by_hash(list(reversed(ids)), 7)
    assert len(kept1) == 7 and len(dropped1) == 13
    # Input order must not change which traces win.
    assert set(kept1) == set(kept2)
    assert set(dropped1) == set(dropped2)
    # Zero budget drops everyone; oversized budget keeps everyone.
    kept, dropped = select_by_hash(ids, 0)
    assert kept == [] and len(dropped) == 20
    kept, dropped = select_by_hash(ids, 100)
    assert len(kept) == 20 and dropped == []


# ----------------------------------------------------------------- integrity

def test_integrity_clean_tree_is_valid():
    rows = [
        _row("root", 0, 100, parent=None),
        _row("a", 10, 90, parent="root"),
        _row("b", 20, 30, parent="a"),
        # Endpoints are inclusive: touching the parent's edges is fine.
        _row("c", 10, 90, parent="root"),
        # Zero-length span inside the parent is fine.
        _row("instant", 50, 50, parent="a"),
    ]
    assert check_integrity(rows) == []


def test_integrity_empty_snapshot_is_valid():
    assert check_integrity([]) == []


def test_integrity_missing_parent_is_flagged_with_span_id():
    rows = [_row("orphan", 0, 10, parent="ghost")]
    anomalies = check_integrity(rows)
    assert len(anomalies) == 1
    a = anomalies[0]
    assert a["type"] == MISSING_PARENT
    assert a["span_ids"] == ["orphan"]
    assert a["parent_span_id"] == "ghost"
    assert "ghost" in a["message"]


def test_integrity_self_reference_is_its_own_anomaly():
    rows = [_row("loop", 0, 10, parent="loop")]
    anomalies = check_integrity(rows)
    assert len(anomalies) == 1
    a = anomalies[0]
    assert a["type"] == SELF_REFERENCE
    assert a["span_ids"] == ["loop"]
    assert a["parent_span_id"] == "loop"


def test_integrity_two_node_cycle_reported_once():
    rows = [
        _row("a", 0, 10, parent="b"),
        _row("b", 0, 10, parent="a"),
    ]
    anomalies = check_integrity(rows)
    assert len(anomalies) == 1
    a = anomalies[0]
    assert a["type"] == CYCLE
    # Canonical rotation starts at the smallest span id, ring order kept.
    assert a["span_ids"] == ["a", "b"]
    assert a["message"] == "cycle of 2 spans detected: a -> b -> a"


def test_integrity_three_node_cycle_with_lead_in_chain_reported_once():
    rows = [
        # lead-in chain reaches the cycle but adds no second anomaly
        _row("in", 0, 10, parent="x"),
        _row("x", 0, 10, parent="y"),
        _row("y", 0, 10, parent="z"),
        _row("z", 0, 10, parent="x"),
    ]
    anomalies = check_integrity(rows)
    assert [a["type"] for a in anomalies] == [CYCLE]
    assert anomalies[0]["span_ids"] == ["x", "y", "z"]


def test_integrity_multiple_disjoint_cycles_each_reported_once():
    rows = [
        _row("a", 0, 10, parent="b"),
        _row("b", 0, 10, parent="a"),
        _row("c", 0, 10, parent="d"),
        _row("d", 0, 10, parent="e"),
        _row("e", 0, 10, parent="c"),
    ]
    anomalies = check_integrity(rows)
    assert [a["type"] for a in anomalies] == [CYCLE, CYCLE]
    assert [a["span_ids"] for a in anomalies] == [
        ["a", "b"], ["c", "d", "e"],
    ]


def test_integrity_cycle_edges_are_not_time_checked():
    # A ring whose intervals do not contain one another: only the cycle is
    # reported, no time_out_of_bounds noise for the ring edges.
    rows = [
        _row("a", 0, 100, parent="b"),
        _row("b", 200, 300, parent="a"),
    ]
    anomalies = check_integrity(rows)
    assert [a["type"] for a in anomalies] == [CYCLE]


def test_integrity_child_outside_parent_interval_is_flagged():
    rows = [
        _row("root", 10, 100),
        # starts before and ends after the parent
        _row("wide", 0, 120, parent="root"),
    ]
    anomalies = check_integrity(rows)
    assert len(anomalies) == 1
    a = anomalies[0]
    assert a["type"] == TIME_OUT_OF_BOUNDS
    assert a["span_ids"] == ["wide", "root"]
    assert a["parent_span_id"] == "root"
    assert a["child_start_ns"] == 0
    assert a["child_end_ns"] == 120
    assert a["parent_start_ns"] == 10
    assert a["parent_end_ns"] == 100
    assert a["start_overflow_ns"] == 10
    assert a["end_overflow_ns"] == 20
    assert "before its parent" in a["message"]
    assert "after its parent" in a["message"]


def test_integrity_start_and_end_overflow_reported_separately():
    rows = [
        _row("p", 100, 200),
        _row("early", 90, 150, parent="p"),
        _row("late", 150, 250, parent="p"),
        # Exactly contained on both edges: valid.
        _row("exact", 100, 200, parent="p"),
    ]
    anomalies = check_integrity(rows)
    assert [a["span_ids"] for a in anomalies] == [
        ["early", "p"], ["late", "p"],
    ]
    early, late = anomalies
    assert early["start_overflow_ns"] == 10
    assert early["end_overflow_ns"] == 0
    assert "starts before" in early["message"] and "ends after" not in (
        early["message"]
    )
    assert late["start_overflow_ns"] == 0
    assert late["end_overflow_ns"] == 50
    assert "ends after" in late["message"]


def test_integrity_orphan_edge_is_not_time_checked():
    # Parent never arrived: missing_parent only; there is no parent interval
    # to check containment against.
    rows = [_row("orphan", 0, 500, parent="ghost")]
    anomalies = check_integrity(rows)
    assert [a["type"] for a in anomalies] == [MISSING_PARENT]


def test_integrity_anomaly_ordering_is_deterministic():
    rows = [
        _row("root", 0, 100),
        _row("self", 0, 10, parent="self"),
        _row("orphan", 0, 10, parent="ghost"),
        _row("c1", 0, 10, parent="c2"),
        _row("c2", 0, 10, parent="c1"),
        _row("wide", 1000, 2000, parent="root"),
    ]
    types = [a["type"] for a in check_integrity(rows)]
    assert types == [
        MISSING_PARENT, SELF_REFERENCE, CYCLE, TIME_OUT_OF_BOUNDS,
    ]
    # Re-running yields identical output.
    assert check_integrity(rows) == check_integrity(list(rows))


# ------------------------------------------------------------ dominant path

def test_dominant_path_single_span_is_its_own_chain():
    assert dominant_path([_row("only", 5, 40)]) == {
        "path": ["only"],
        "weights_ns": [35],
        "total_weight_ns": 35,
    }


def test_dominant_path_empty_snapshot():
    assert dominant_path([]) == {
        "path": [],
        "weights_ns": [],
        "total_weight_ns": 0,
    }


def test_dominant_path_picks_heaviest_branch():
    rows = [
        _row("root", 0, 100),
        _row("a", 10, 50, parent="root"),   # weight 40
        _row("b", 60, 95, parent="root"),   # weight 35
    ]
    # root: 100 - 75 = 25; root->a = 65 beats root->b = 60.
    assert dominant_path(rows) == {
        "path": ["root", "a"],
        "weights_ns": [25, 40],
        "total_weight_ns": 65,
    }


def test_dominant_path_nested_chain_sums_to_wall_clock():
    rows = [
        _row("root", 0, 100),
        _row("mid", 10, 90, parent="root"),
        _row("leaf", 20, 80, parent="mid"),
        _row("side", 40, 60, parent="root"),
    ]
    # root 20, mid 20, leaf 60 -> 100 beats root->side = 40.
    assert dominant_path(rows) == {
        "path": ["root", "mid", "leaf"],
        "weights_ns": [20, 20, 60],
        "total_weight_ns": 100,
    }


def test_dominant_path_overlapping_children_subtracted_once():
    rows = [
        _row("root", 0, 100),
        _row("c1", 10, 60, parent="root"),
        _row("c2", 50, 80, parent="root"),
    ]
    # Union [10, 80) counts once: root weight 100 - 70 = 30.
    assert dominant_path(rows) == {
        "path": ["root", "c1"],
        "weights_ns": [30, 50],
        "total_weight_ns": 80,
    }


def test_dominant_path_out_of_bounds_child_is_clipped():
    rows = [
        _row("root", 10, 50),
        _row("wide", 0, 100, parent="root"),
    ]
    # Only the part inside the parent is deducted: root weight 0.
    assert dominant_path(rows) == {
        "path": ["root", "wide"],
        "weights_ns": [0, 100],
        "total_weight_ns": 100,
    }


def test_dominant_path_missing_parent_span_becomes_a_root():
    rows = [
        _row("orphan", 0, 100, parent="ghost"),
        _row("c", 10, 60, parent="orphan"),
        _row("r", 0, 80),
    ]
    # orphan->c = 50 + 50 = 100 beats the real root r = 80.
    assert dominant_path(rows) == {
        "path": ["orphan", "c"],
        "weights_ns": [50, 50],
        "total_weight_ns": 100,
    }


def test_dominant_path_tie_breaks_to_lexicographically_smallest_sequence():
    rows = [
        _row("root", 0, 100),
        _row("b", 0, 50, parent="root"),
        _row("a", 50, 100, parent="root"),
    ]
    # root weight 0; both branches total 50 -> ["root", "a"] wins.
    assert dominant_path(rows) == {
        "path": ["root", "a"],
        "weights_ns": [0, 50],
        "total_weight_ns": 50,
    }


def test_dominant_path_tie_between_roots():
    rows = [
        _row("b", 0, 10),
        _row("a", 0, 10),
    ]
    assert dominant_path(rows) == {
        "path": ["a"],
        "weights_ns": [10],
        "total_weight_ns": 10,
    }


def test_dominant_path_zero_duration_span_weighs_zero():
    rows = [
        _row("root", 0, 100),
        _row("instant", 50, 50, parent="root"),
    ]
    # The empty child interval deducts nothing and adds nothing.
    assert dominant_path(rows) == {
        "path": ["root", "instant"],
        "weights_ns": [100, 0],
        "total_weight_ns": 100,
    }


def test_dominant_path_self_reference_raises_with_span_ids():
    rows = [
        _row("root", 0, 100),
        _row("loop", 10, 20, parent="loop"),
    ]
    with pytest.raises(CyclicTraceError) as excinfo:
        dominant_path(rows)
    exc = excinfo.value
    assert exc.span_ids == ["loop"]
    assert exc.self_references == ["loop"]
    assert exc.cycles == []
    assert "loop" in str(exc)


def test_dominant_path_cycle_raises_with_ring_members_only():
    rows = [
        # Lead-in chain reaches the ring but is not part of it.
        _row("in", 0, 5, parent="x"),
        _row("x", 0, 10, parent="y"),
        _row("y", 0, 10, parent="z"),
        _row("z", 0, 10, parent="x"),
    ]
    with pytest.raises(CyclicTraceError) as excinfo:
        dominant_path(rows)
    exc = excinfo.value
    assert exc.span_ids == ["x", "y", "z"]
    assert exc.self_references == []
    assert exc.cycles == [["x", "y", "z"]]


def test_dominant_path_self_reference_and_cycle_reported_together():
    rows = [
        _row("loop", 0, 10, parent="loop"),
        _row("a", 0, 10, parent="b"),
        _row("b", 0, 10, parent="a"),
        _row("clean", 0, 10),
    ]
    with pytest.raises(CyclicTraceError) as excinfo:
        dominant_path(rows)
    exc = excinfo.value
    assert exc.span_ids == ["a", "b", "loop"]
    assert exc.self_references == ["loop"]
    assert exc.cycles == [["a", "b"]]


# ------------------------------------------------------------ service edges

def _edges(rows):
    return {(e["source_service"], e["target_service"]): e
            for e in service_edges(rows)}


def test_service_edges_empty_snapshot():
    assert service_edges([]) == []


def test_service_edges_basic_cross_service_call():
    rows = [
        _row("root", 0, 100, service="frontend"),
        _row("child", 10, 40, parent="root", service="backend"),
    ]
    assert service_edges(rows) == [{
        "source_service": "frontend",
        "target_service": "backend",
        "call_count": 1,
        "duration_ns": 30,
    }]


def test_service_edges_same_or_missing_service_is_ignored():
    rows = [
        _row("root", 0, 100, service="a"),
        # Same service as the parent: not a cross-service edge.
        _row("same", 0, 50, parent="root", service="a"),
        # Child without a service.
        _row("anon", 0, 50, parent="root", service=None),
        # Parent without a service.
        _row("named", 0, 50, parent="anon", service="b"),
    ]
    assert service_edges(rows) == []


def test_service_edges_missing_parent_and_self_reference_are_ignored():
    rows = [
        _row("orphan", 0, 50, parent="ghost", service="b"),
        _row("loop", 0, 50, parent="loop", service="a"),
    ]
    assert service_edges(rows) == []


def test_service_edges_overlapping_children_union_once_per_parent():
    rows = [
        _row("root", 0, 100, service="a"),
        _row("c1", 10, 60, parent="root", service="b"),
        _row("c2", 50, 80, parent="root", service="b"),
    ]
    edge = _edges(rows)[("a", "b")]
    assert edge["call_count"] == 2
    # Union of [10, 60) and [50, 80) counts the overlap only once.
    assert edge["duration_ns"] == 70


def test_service_edges_union_is_per_target_service():
    rows = [
        _row("root", 0, 100, service="a"),
        _row("c1", 10, 60, parent="root", service="b"),
        _row("c2", 50, 80, parent="root", service="c"),
    ]
    edges = _edges(rows)
    # The [50, 60) overlap counts towards both edges: unions are per target.
    assert edges[("a", "b")]["duration_ns"] == 50
    assert edges[("a", "c")]["duration_ns"] == 30


def test_service_edges_child_is_clipped_to_parent_interval():
    rows = [
        _row("root", 10, 50, service="a"),
        _row("wide", 0, 100, parent="root", service="b"),
    ]
    assert _edges(rows)[("a", "b")]["duration_ns"] == 40


def test_service_edges_zero_length_intersection_adds_no_duration():
    rows = [
        _row("root", 0, 100, service="a"),
        _row("instant", 50, 50, parent="root", service="b"),
        # Entirely outside the parent interval: empty intersection.
        _row("outside", 200, 300, parent="root", service="b"),
    ]
    edge = _edges(rows)[("a", "b")]
    # Both calls count towards the edge; neither contributes duration.
    assert edge["call_count"] == 2
    assert edge["duration_ns"] == 0


def test_service_edges_duration_sums_across_parent_spans():
    rows = [
        _row("p1", 0, 100, service="a"),
        _row("c1", 10, 40, parent="p1", service="b"),
        _row("p2", 0, 100, service="a"),
        _row("c2", 10, 40, parent="p2", service="b"),
    ]
    edge = _edges(rows)[("a", "b")]
    assert edge["call_count"] == 2
    assert edge["duration_ns"] == 60


def test_service_edges_sorting_duration_desc_then_names_asc():
    rows = [
        _row("r1", 0, 100, service="zsvc"),
        _row("c1", 0, 20, parent="r1", service="bsvc"),
        _row("r2", 0, 100, service="asvc"),
        _row("c2", 0, 20, parent="r2", service="bsvc"),
        _row("r3", 0, 100, service="msvc"),
        _row("c3", 0, 90, parent="r3", service="bsvc"),
    ]
    result = service_edges(rows)
    assert [
        (e["source_service"], e["target_service"], e["duration_ns"])
        for e in result
    ] == [
        ("msvc", "bsvc", 90),
        ("asvc", "bsvc", 20),   # duration tie: source name ascending
        ("zsvc", "bsvc", 20),
    ]
