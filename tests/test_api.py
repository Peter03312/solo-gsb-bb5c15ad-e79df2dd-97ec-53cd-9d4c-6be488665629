"""End-to-end API behavior for the tail sampling service."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.sampler import select_by_hash, stable_hash


BASE = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def _iso(minute: int, second: float = 0.0) -> str:
    return (BASE + timedelta(minutes=minute, seconds=second)).isoformat().replace(
        "+00:00", "Z"
    )


def _span(trace_id: str, *, minute: int, status: str = "ok",
          duration_ms: int = 10, span_id: str | None = None,
          attributes=None) -> dict:
    start = _iso(minute)
    end = (
        BASE + timedelta(minutes=minute, milliseconds=duration_ms)
    ).isoformat().replace("+00:00", "Z")
    return {
        "trace_id": trace_id,
        "span_id": span_id or f"span-{trace_id}-1",
        "name": "root",
        "service": "svc-a",
        "start_time": start,
        "end_time": end,
        "status": status,
        "attributes": attributes or {},
    }


def test_health_and_config(client):
    assert client.get("/health").json() == {"status": "ok"}
    cfg = client.get("/api/v1/config").json()
    assert cfg["sample_budget_per_minute"] == 5
    assert cfg["slow_threshold_ns"] == 1_000_000_000


def test_unknown_trace_returns_404(client):
    resp = client.get("/api/v1/decisions/does-not-exist")
    assert resp.status_code == 404


def test_error_traces_have_priority_and_consume_budget(client):
    # Budget is 5/minute; submit 7 error traces all in minute 10.
    spans = [
        _span(f"err-{i}", minute=10, status="error", span_id=f"s{i}")
        for i in range(7)
    ]
    resp = client.post("/api/v1/spans:batch", json={"spans": spans})
    body = resp.json()
    assert body["accepted"] == 7

    # Before closing, traces are still pending.
    d0 = client.get("/api/v1/decisions/err-0").json()
    assert d0["decision"] == "pending"

    client.post("/api/v1/admin/force-flush")

    kept, dropped = [], []
    for i in range(7):
        d = client.get(f"/api/v1/decisions/err-{i}").json()
        (kept if d["decision"] == "keep" else dropped).append(d)

    assert len(kept) == 5
    assert len(dropped) == 2
    assert all(d["reason"] == "error_trace" for d in kept)
    assert all(d["state"] == "priority_kept" for d in kept)
    assert all(d["reason"] == "priority_budget_exhausted" for d in dropped)
    assert all(d["is_error"] is True for d in kept + dropped)


def test_slow_trace_kept_and_attribute_error_recognized(client):
    # Slow: 2 seconds > 1s threshold (minute 20).
    client.post("/api/v1/spans:batch", json={"spans": [
        _span("slow-1", minute=20, duration_ms=2000, span_id="sl")
    ]})
    # Error signalled only via conventional attributes.
    client.post("/api/v1/spans:batch", json={"spans": [
        _span("http-err", minute=20, span_id="he",
              attributes={"http.status_code": 503})
    ]})
    # Fast and healthy.
    client.post("/api/v1/spans:batch", json={"spans": [
        _span("fast-1", minute=20, duration_ms=5, span_id="fa")
    ]})
    client.post("/api/v1/admin/force-flush")

    slow = client.get("/api/v1/decisions/slow-1").json()
    assert slow["decision"] == "keep"
    assert slow["reason"] == "slow_trace"
    assert slow["is_slow"] is True
    assert slow["duration_ns"] == 2_000_000_000

    http_err = client.get("/api/v1/decisions/http-err").json()
    assert http_err["decision"] == "keep"
    assert http_err["reason"] == "error_trace"

    fast = client.get("/api/v1/decisions/fast-1").json()
    # Two priority traces consumed 2 of 5 slots; the lone normal trace fills
    # one of the remaining slots through the hash path.
    assert fast["decision"] == "keep"
    assert fast["reason"] == "stable_hash_selected"


def test_normal_traces_fill_remaining_budget_by_stable_hash(client):
    # Clean minute 30 with 12 normal traces, budget 5.
    spans = [
        _span(f"norm-{i}", minute=30, duration_ms=2, span_id=f"n{i}")
        for i in range(12)
    ]
    client.post("/api/v1/spans:batch", json={"spans": spans})
    client.post("/api/v1/admin/force-flush")

    kept, dropped = [], []
    for i in range(12):
        d = client.get(f"/api/v1/decisions/norm-{i}").json()
        (kept if d["decision"] == "keep" else dropped).append(
            (d["trace_id"], d)
        )

    assert len(kept) == 5
    assert len(dropped) == 7
    assert all(d["reason"] == "stable_hash_selected" for _, d in kept)
    assert all(
        d["reason"] == "stable_hash_not_selected" for _, d in dropped
    )

    # The hash winners must equal the deterministic rank selection.
    expected_kept, _ = select_by_hash([f"norm-{i}" for i in range(12)], 5)
    assert {tid for tid, _ in kept} == set(expected_kept)


def test_stable_hash_is_deterministic(client):
    ids = [f"h-{i}" for i in range(50)]
    kept_a, drop_a = select_by_hash(ids, 7)
    kept_b, drop_b = select_by_hash(ids, 7)
    assert kept_a == kept_b
    assert drop_a == drop_b
    assert len(kept_a) == 7
    # Same trace id always maps to the same hash.
    assert stable_hash("abc") == stable_hash("abc")
    assert 0 <= stable_hash("abc") < 2**64


def test_duplicate_spans_and_late_spans_are_rejected(client):
    span = _span("dup-1", minute=40, span_id="only")
    r1 = client.post("/api/v1/spans:batch", json={"spans": [span]})
    assert r1.json()["accepted"] == 1
    r2 = client.post("/api/v1/spans:batch", json={"spans": [span]})
    assert r2.json()["accepted"] == 0
    assert r2.json()["duplicates"] == 1

    # Close the trace, then try to append a new span: immutable decision.
    client.post("/api/v1/admin/force-flush")
    late = _span("dup-1", minute=40, span_id="late")
    r3 = client.post("/api/v1/spans:batch", json={"spans": [late]})
    assert r3.json()["accepted"] == 0
    assert r3.json()["rejected"] == 1


def test_batch_decision_query_and_stats(client):
    resp = client.post(
        "/api/v1/decisions:batch",
        json={"trace_ids": ["norm-0", "norm-1", "missing-x"]},
    )
    body = resp.json()
    assert "norm-0" in body["decisions"]
    assert "norm-1" in body["decisions"]
    assert body["missing"] == ["missing-x"]

    stats = client.get("/api/v1/stats").json()
    assert stats["open_traces"] == 0
    assert stats["pending_normal"] == 0
    assert stats["retained"] > 0
    assert stats["dropped"] > 0
    assert stats["decided_traces"] == stats["retained"] + stats["dropped"]


def _custom_span(trace_id, span_id, start_ms, end_ms, *, service,
                 parent_span_id=None):
    # Integer epoch ns: exact, avoids ISO float-rounding in comparisons.
    base_ns = 1577836800 * 10**9 + 60 * 60 * 10**9  # BASE + 60 minutes
    return {
        "trace_id": trace_id,
        "span_id": span_id,
        "parent_span_id": parent_span_id,
        "name": "op",
        "service": service,
        "start_time": base_ns + start_ms * 10**6,
        "end_time": base_ns + end_ms * 10**6,
        "status": "ok",
        "attributes": {},
    }


def test_service_time_unknown_trace_returns_404(client):
    resp = client.get("/api/v1/traces/no-such-trace/service-time")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "trace not found"


def test_service_time_for_open_trace_uses_current_snapshot(client):
    tid = "st-open"
    client.post("/api/v1/spans:batch", json={"spans": [
        _custom_span(tid, "root", 0, 100, service="gateway"),
        _custom_span(tid, "a", 10, 50, service="auth", parent_span_id="root"),
    ]})

    # Open traces are queryable from the spans received so far:
    # root 100 - 40 = 60 (gateway), child leaf = 40 (auth).
    resp = client.get(f"/api/v1/traces/{tid}/service-time")
    assert resp.status_code == 200
    body = resp.json()
    assert body["trace_id"] == tid
    assert body["span_count"] == 2
    assert body["services"] == [
        {"service": "gateway", "span_count": 1, "exclusive_ns": 60_000_000},
        {"service": "auth", "span_count": 1, "exclusive_ns": 40_000_000},
    ]

    # A newly received child extends the consistent snapshot on re-query.
    client.post("/api/v1/spans:batch", json={"spans": [
        _custom_span(tid, "b", 50, 90, service="auth", parent_span_id="root"),
    ]})
    body = client.get(f"/api/v1/traces/{tid}/service-time").json()
    assert body["span_count"] == 3
    assert body["services"] == [
        {"service": "auth", "span_count": 2, "exclusive_ns": 80_000_000},
        {"service": "gateway", "span_count": 1, "exclusive_ns": 20_000_000},
    ]


def test_service_time_after_close_handles_null_group_and_overlaps(client):
    tid = "st-closed"
    client.post("/api/v1/spans:batch", json={"spans": [
        _custom_span(tid, "root", 0, 100, service="a", parent_span_id=None),
        # Two overlapping children: union [10, 80) subtracted only once.
        _custom_span(tid, "c1", 10, 60, service="b", parent_span_id="root"),
        _custom_span(tid, "c2", 50, 80, service="b", parent_span_id="root"),
        # Self-referencing parent-child: ignored entirely.
        _custom_span(tid, "loop", 90, 95, service="a", parent_span_id="loop"),
        # Missing service -> null group.
        _custom_span(tid, "anon", 95, 100, service=None,
                     parent_span_id="root"),
    ]})
    client.post("/api/v1/admin/force-flush")

    resp = client.get(f"/api/v1/traces/{tid}/service-time")
    assert resp.status_code == 200
    services = resp.json()["services"]
    # a: root (100 - 75 = 25) + loop (5) = 30; b: 50 + 30 = 80; null: 5.
    assert services == [
        {"service": "b", "span_count": 2, "exclusive_ns": 80_000_000},
        {"service": "a", "span_count": 2, "exclusive_ns": 30_000_000},
        {"service": None, "span_count": 1, "exclusive_ns": 5_000_000},
    ]


def test_concurrency_unknown_trace_returns_404(client):
    resp = client.get("/api/v1/traces/no-such-trace/concurrency")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "trace not found"


def test_concurrency_min_active_below_two_is_422(client):
    tid = "cc-validation"
    client.post("/api/v1/spans:batch", json={"spans": [
        _custom_span(tid, "a", 0, 100, service="svc"),
    ]})
    resp = client.get(f"/api/v1/traces/{tid}/concurrency?min_active=1")
    assert resp.status_code == 422
    # Non-integer thresholds are rejected the same way.
    resp = client.get(f"/api/v1/traces/{tid}/concurrency?min_active=abc")
    assert resp.status_code == 422


def test_concurrency_windows_peak_and_total(client):
    tid = "cc-basic"
    client.post("/api/v1/spans:batch", json={"spans": [
        _custom_span(tid, "a", 0, 100, service="svc"),
        _custom_span(tid, "b", 20, 80, service="svc"),
        _custom_span(tid, "c", 40, 60, service="svc"),
        # Zero-duration span: must not affect the sweep.
        _custom_span(tid, "instant", 50, 50, service="svc"),
        # Touching endpoints: starts exactly where a ends, no overlap with a.
        _custom_span(tid, "tail", 100, 150, service="svc"),
    ]})

    resp = client.get(f"/api/v1/traces/{tid}/concurrency")
    assert resp.status_code == 200
    body = resp.json()
    assert body["trace_id"] == tid
    assert body["min_active"] == 2
    assert body["span_count"] == 5
    # Overlap of a/b/c is [20ms, 80ms) with peak 3 over [40ms, 60ms).
    ms = 10**6
    base_ns = 1577836800 * 10**9 + 60 * 60 * 10**9
    assert body["windows"] == [{
        "start_ns": base_ns + 20 * ms,
        "end_ns": base_ns + 80 * ms,
        "duration_ns": 60 * ms,
        "peak_active": 3,
    }]
    assert body["window_count"] == 1
    assert body["total_concurrent_ns"] == 60 * ms

    # A stricter threshold keeps only the triple-overlap core.
    body = client.get(
        f"/api/v1/traces/{tid}/concurrency?min_active=3"
    ).json()
    assert body["min_active"] == 3
    assert body["windows"] == [{
        "start_ns": base_ns + 40 * ms,
        "end_ns": base_ns + 60 * ms,
        "duration_ns": 20 * ms,
        "peak_active": 3,
    }]
    assert body["total_concurrent_ns"] == 20 * ms


def test_concurrency_merges_adjacent_windows_and_follows_open_trace(client):
    tid = "cc-open"
    client.post("/api/v1/spans:batch", json={"spans": [
        _custom_span(tid, "a", 0, 100, service="svc"),
        _custom_span(tid, "b", 50, 150, service="svc"),
    ]})
    ms = 10**6
    base_ns = 1577836800 * 10**9 + 60 * 60 * 10**9

    # Open trace: snapshot of what has arrived so far.
    body = client.get(f"/api/v1/traces/{tid}/concurrency").json()
    assert body["windows"] == [{
        "start_ns": base_ns + 50 * ms,
        "end_ns": base_ns + 100 * ms,
        "duration_ns": 50 * ms,
        "peak_active": 2,
    }]

    # A new span starting exactly where "a" ends extends the qualifying
    # stretch continuously (b stays active across the seam at 100ms).
    client.post("/api/v1/spans:batch", json={"spans": [
        _custom_span(tid, "c", 100, 120, service="svc"),
    ]})
    body = client.get(f"/api/v1/traces/{tid}/concurrency").json()
    assert body["span_count"] == 3
    assert body["windows"] == [{
        "start_ns": base_ns + 50 * ms,
        "end_ns": base_ns + 120 * ms,
        "duration_ns": 70 * ms,
        "peak_active": 2,
    }]
    assert body["total_concurrent_ns"] == 70 * ms


def test_concurrency_works_after_close(client):
    tid = "cc-closed"
    client.post("/api/v1/spans:batch", json={"spans": [
        _custom_span(tid, "a", 0, 60, service="svc"),
        _custom_span(tid, "b", 30, 90, service="svc"),
    ]})
    client.post("/api/v1/admin/force-flush")

    resp = client.get(f"/api/v1/traces/{tid}/concurrency?min_active=2")
    assert resp.status_code == 200
    body = resp.json()
    ms = 10**6
    base_ns = 1577836800 * 10**9 + 60 * 60 * 10**9
    assert body["windows"] == [{
        "start_ns": base_ns + 30 * ms,
        "end_ns": base_ns + 60 * ms,
        "duration_ns": 30 * ms,
        "peak_active": 2,
    }]


def test_integrity_unknown_trace_returns_404(client):
    resp = client.get("/api/v1/traces/no-such-trace/integrity")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "trace not found"


def test_integrity_clean_open_trace_is_valid_and_follows_snapshot(client):
    tid = "ig-open"
    client.post("/api/v1/spans:batch", json={"spans": [
        _custom_span(tid, "root", 0, 100, service="gateway"),
        _custom_span(tid, "a", 10, 90, service="auth", parent_span_id="root"),
    ]})

    resp = client.get(f"/api/v1/traces/{tid}/integrity")
    assert resp.status_code == 200
    body = resp.json()
    assert body["trace_id"] == tid
    assert body["span_count"] == 2
    assert body["anomaly_count"] == 0
    assert body["valid"] is True
    assert body["anomalies"] == []

    # A later batch with a bad span flips the snapshot to invalid.
    client.post("/api/v1/spans:batch", json={"spans": [
        _custom_span(tid, "bad", 200, 300, service="auth",
                     parent_span_id="root"),
    ]})
    body = client.get(f"/api/v1/traces/{tid}/integrity").json()
    assert body["span_count"] == 3
    assert body["valid"] is False
    assert body["anomaly_count"] == 1
    anomaly = body["anomalies"][0]
    assert anomaly["type"] == "time_out_of_bounds"
    assert anomaly["span_ids"] == ["bad", "root"]
    assert anomaly["parent_span_id"] == "root"
    ms = 10**6
    assert anomaly["start_overflow_ns"] == 0
    assert anomaly["end_overflow_ns"] == 200 * ms


def test_integrity_reports_missing_parent_self_reference_and_cycle_once(client):
    tid = "ig-refs"
    client.post("/api/v1/spans:batch", json={"spans": [
        _custom_span(tid, "root", 0, 100, service="svc"),
        # Parent never received.
        _custom_span(tid, "orphan", 0, 10, service="svc",
                     parent_span_id="ghost"),
        # Self reference.
        _custom_span(tid, "self", 0, 10, service="svc",
                     parent_span_id="self"),
        # Two-node ring.
        _custom_span(tid, "c1", 0, 10, service="svc", parent_span_id="c2"),
        _custom_span(tid, "c2", 0, 10, service="svc", parent_span_id="c1"),
    ]})

    body = client.get(f"/api/v1/traces/{tid}/integrity").json()
    assert body["span_count"] == 5
    assert body["valid"] is False
    anomalies = body["anomalies"]
    assert body["anomaly_count"] == len(anomalies) == 3

    by_type = {a["type"]: a for a in anomalies}
    assert by_type["missing_parent"]["span_ids"] == ["orphan"]
    assert by_type["missing_parent"]["parent_span_id"] == "ghost"
    assert by_type["self_reference"]["span_ids"] == ["self"]
    # The c1<->c2 ring shows up exactly once with both members.
    assert by_type["cycle"]["span_ids"] == ["c1", "c2"]
    assert by_type["cycle"]["parent_span_id"] is None


def test_integrity_works_after_trace_close(client):
    tid = "ig-closed"
    client.post("/api/v1/spans:batch", json={"spans": [
        _custom_span(tid, "root", 0, 100, service="svc"),
        _custom_span(tid, "child", 0, 120, service="svc",
                     parent_span_id="root"),
    ]})
    client.post("/api/v1/admin/force-flush")

    body = client.get(f"/api/v1/traces/{tid}/integrity").json()
    assert body["span_count"] == 2
    assert body["valid"] is False
    (anomaly,) = body["anomalies"]
    assert anomaly["type"] == "time_out_of_bounds"
    assert anomaly["span_ids"] == ["child", "root"]
    assert anomaly["end_overflow_ns"] == 20 * 10**6
    assert anomaly["start_overflow_ns"] == 0


def test_service_edges_unknown_trace_returns_404(client):
    resp = client.get("/api/v1/traces/no-such-trace/service-edges")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "trace not found"


def test_service_edges_open_trace_follows_snapshot(client):
    tid = "se-open"
    client.post("/api/v1/spans:batch", json={"spans": [
        _custom_span(tid, "root", 0, 100, service="gateway"),
        _custom_span(tid, "a", 10, 50, service="auth", parent_span_id="root"),
        _custom_span(tid, "b", 20, 40, service="auth", parent_span_id="root"),
        # Same service as the parent: no edge.
        _custom_span(tid, "in", 50, 60, service="gateway",
                     parent_span_id="root"),
        # Parent never arrived: no edge.
        _custom_span(tid, "orphan", 0, 10, service="auth",
                     parent_span_id="ghost"),
        # Missing service: no edge.
        _custom_span(tid, "anon", 60, 70, service=None, parent_span_id="root"),
    ]})

    ms = 10**6
    resp = client.get(f"/api/v1/traces/{tid}/service-edges")
    assert resp.status_code == 200
    body = resp.json()
    assert body["trace_id"] == tid
    assert body["span_count"] == 6
    # gateway->auth: 2 calls; children [10,50) and [20,40) clipped to root
    # [0,100) and unioned -> [10,50) = 40ms.
    assert body["edge_count"] == 1
    assert body["edges"] == [{
        "source_service": "gateway",
        "target_service": "auth",
        "call_count": 2,
        "duration_ns": 40 * ms,
    }]

    # Newly received spans extend the consistent snapshot on re-query.
    client.post("/api/v1/spans:batch", json={"spans": [
        _custom_span(tid, "p", 60, 90, service="payments",
                     parent_span_id="root"),
        _custom_span(tid, "db", 65, 80, service="payments-db",
                     parent_span_id="p"),
    ]})
    body = client.get(f"/api/v1/traces/{tid}/service-edges").json()
    assert body["span_count"] == 8
    assert body["edge_count"] == 3
    assert body["edges"] == [
        {"source_service": "gateway", "target_service": "auth",
         "call_count": 2, "duration_ns": 40 * ms},
        {"source_service": "gateway", "target_service": "payments",
         "call_count": 1, "duration_ns": 30 * ms},
        {"source_service": "payments", "target_service": "payments-db",
         "call_count": 1, "duration_ns": 15 * ms},
    ]


def test_service_edges_after_close_clips_and_skips_empty_intersections(client):
    tid = "se-closed"
    client.post("/api/v1/spans:batch", json={"spans": [
        _custom_span(tid, "root", 10, 50, service="a"),
        # Sticks out of the parent: only [10, 50) contributes.
        _custom_span(tid, "wide", 0, 100, service="b", parent_span_id="root"),
        # Zero-length span: counts as a call, adds no duration.
        _custom_span(tid, "instant", 20, 20, service="b",
                     parent_span_id="root"),
        # Self reference: ignored entirely.
        _custom_span(tid, "loop", 30, 40, service="c", parent_span_id="loop"),
    ]})
    client.post("/api/v1/admin/force-flush")

    ms = 10**6
    resp = client.get(f"/api/v1/traces/{tid}/service-edges")
    assert resp.status_code == 200
    body = resp.json()
    assert body["span_count"] == 4
    assert body["edges"] == [{
        "source_service": "a",
        "target_service": "b",
        "call_count": 2,
        "duration_ns": 40 * ms,
    }]


def test_dominant_path_unknown_trace_returns_404(client):
    resp = client.get("/api/v1/traces/no-such-trace/dominant-path")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "trace not found"


def test_dominant_path_open_trace_follows_snapshot(client):
    tid = "dp-open"
    client.post("/api/v1/spans:batch", json={"spans": [
        _custom_span(tid, "root", 0, 100, service="gateway"),
        _custom_span(tid, "a", 10, 50, service="auth", parent_span_id="root"),
    ]})

    ms = 10**6
    resp = client.get(f"/api/v1/traces/{tid}/dominant-path")
    assert resp.status_code == 200
    body = resp.json()
    assert body["trace_id"] == tid
    assert body["span_count"] == 2
    # root: 100 - 40 = 60; a: 40 -> total 100.
    assert body["path"] == ["root", "a"]
    assert body["weights_ns"] == [60 * ms, 40 * ms]
    assert body["total_weight_ns"] == 100 * ms

    # A heavier second branch flips the dominant chain on re-query.
    client.post("/api/v1/spans:batch", json={"spans": [
        _custom_span(tid, "b", 50, 95, service="billing",
                     parent_span_id="root"),
        _custom_span(tid, "b1", 55, 90, service="billing-db",
                     parent_span_id="b"),
    ]})
    body = client.get(f"/api/v1/traces/{tid}/dominant-path").json()
    assert body["span_count"] == 4
    # root: 100 - 85 = 15; b: 45 - 35 = 10; b1: 35 -> 60 beats root->a = 55.
    assert body["path"] == ["root", "b", "b1"]
    assert body["weights_ns"] == [15 * ms, 10 * ms, 35 * ms]
    assert body["total_weight_ns"] == 60 * ms


def test_dominant_path_tie_breaks_to_smallest_span_id_sequence(client):
    tid = "dp-tie"
    client.post("/api/v1/spans:batch", json={"spans": [
        _custom_span(tid, "root", 0, 100, service="svc"),
        _custom_span(tid, "b", 0, 50, service="svc", parent_span_id="root"),
        _custom_span(tid, "a", 50, 100, service="svc", parent_span_id="root"),
    ]})

    ms = 10**6
    body = client.get(f"/api/v1/traces/{tid}/dominant-path").json()
    # Both branches total 50ms; the lexicographically smaller sequence wins.
    assert body["path"] == ["root", "a"]
    assert body["weights_ns"] == [0, 50 * ms]
    assert body["total_weight_ns"] == 50 * ms


def test_dominant_path_missing_parent_span_is_treated_as_root(client):
    tid = "dp-orphan"
    client.post("/api/v1/spans:batch", json={"spans": [
        _custom_span(tid, "orphan", 0, 100, service="svc",
                     parent_span_id="ghost"),
        _custom_span(tid, "c", 10, 60, service="svc", parent_span_id="orphan"),
        _custom_span(tid, "r", 0, 80, service="svc"),
    ]})

    ms = 10**6
    body = client.get(f"/api/v1/traces/{tid}/dominant-path").json()
    # orphan->c = 50 + 50 = 100 beats the real root r = 80.
    assert body["path"] == ["orphan", "c"]
    assert body["weights_ns"] == [50 * ms, 50 * ms]
    assert body["total_weight_ns"] == 100 * ms


def test_dominant_path_self_reference_is_422_with_span_ids(client):
    tid = "dp-self"
    client.post("/api/v1/spans:batch", json={"spans": [
        _custom_span(tid, "root", 0, 100, service="svc"),
        _custom_span(tid, "loop", 10, 20, service="svc",
                     parent_span_id="loop"),
    ]})

    resp = client.get(f"/api/v1/traces/{tid}/dominant-path")
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert detail["span_ids"] == ["loop"]
    assert detail["self_references"] == ["loop"]
    assert detail["cycles"] == []
    assert "loop" in detail["message"]


def test_dominant_path_cycle_is_422_and_lists_ring_members(client):
    tid = "dp-cycle"
    client.post("/api/v1/spans:batch", json={"spans": [
        # Lead-in chain reaches the ring but is not implicated.
        _custom_span(tid, "in", 0, 5, service="svc", parent_span_id="x"),
        _custom_span(tid, "x", 0, 10, service="svc", parent_span_id="y"),
        _custom_span(tid, "y", 0, 10, service="svc", parent_span_id="z"),
        _custom_span(tid, "z", 0, 10, service="svc", parent_span_id="x"),
    ]})

    resp = client.get(f"/api/v1/traces/{tid}/dominant-path")
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert detail["span_ids"] == ["x", "y", "z"]
    assert detail["self_references"] == []
    assert detail["cycles"] == [["x", "y", "z"]]


def test_dominant_path_works_after_trace_close(client):
    tid = "dp-closed"
    client.post("/api/v1/spans:batch", json={"spans": [
        _custom_span(tid, "root", 0, 100, service="svc"),
        _custom_span(tid, "a", 10, 90, service="svc", parent_span_id="root"),
    ]})
    client.post("/api/v1/admin/force-flush")

    ms = 10**6
    resp = client.get(f"/api/v1/traces/{tid}/dominant-path")
    assert resp.status_code == 200
    body = resp.json()
    # root: 100 - 80 = 20; a: 80 -> total 100.
    assert body["path"] == ["root", "a"]
    assert body["weights_ns"] == [20 * ms, 80 * ms]
    assert body["total_weight_ns"] == 100 * ms


def test_decisions_survive_engine_restart(client, tmp_path):
    from app.config import Settings
    from app.engine import SamplingEngine
    from app.storage import Storage

    db_path = str(tmp_path / "restart.db")
    st = Storage(db_path)
    cfg = Settings()
    cfg.database_path = db_path
    cfg.idle_window_seconds = 3600
    eng = SamplingEngine(cfg, st)
    eng.recover()

    spans_payload = [
        _span("persist-ok", minute=50, status="ok", duration_ms=3,
              span_id="p1"),
        _span("persist-err", minute=50, status="error", span_id="p2"),
    ]
    from app.models import BatchIngestRequest
    eng.ingest(BatchIngestRequest(spans=spans_payload).spans)
    eng.force_flush()
    assert eng.get_decision("persist-err")["decision"] == "keep"
    st.close()

    # Simulate process restart with a brand new engine on the same database.
    st2 = Storage(db_path)
    cfg2 = Settings()
    cfg2.database_path = db_path
    cfg2.idle_window_seconds = 3600
    eng2 = SamplingEngine(cfg2, st2)
    eng2.recover()
    assert eng2.get_decision("persist-err")["reason"] == "error_trace"
    assert eng2.get_decision("persist-ok")["decision"] in ("keep", "drop")
    assert eng2.stats()["open_traces"] == 0
    assert eng2.stats()["pending_normal"] == 0
    st2.close()
