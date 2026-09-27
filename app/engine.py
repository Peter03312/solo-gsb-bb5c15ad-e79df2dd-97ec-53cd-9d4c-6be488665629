"""Tail-sampling engine.

Responsibilities
----------------
* Aggregate spans into complete traces using a silence window.
* When a trace closes, classify it (error / slow priority vs. normal).
* Priority traces are decided immediately against the per-minute budget.
* Normal traces wait until the minute bucket settles (minute end plus the
  silence window, so every priority trace of that minute has been seen) and
  then fill the remaining budget by stable-hash ranking.
* Everything is persisted to SQLite; in-memory maps are only caches that are
  rebuilt on startup.

All public methods are synchronous and guarded by one lock; FastAPI runs the
sync endpoints in its worker threadpool.
"""
from __future__ import annotations

import threading
from typing import Any

from . import concurrency
from . import dominantpath
from . import integrity
from . import sampler
from . import serviceedges
from . import servicetime
from .config import Settings
from .models import SpanIn
from .storage import Storage, _attrs_signal_error
from .timeutil import minute_bucket, now_ns, to_ns


class SamplingEngine:
    def __init__(self, settings: Settings, storage: Storage) -> None:
        self.settings = settings
        self.storage = storage
        self._lock = threading.RLock()
        # trace_id -> last received span ns (cache of open traces).
        self._open: dict[str, int] = {}
        # trace_id -> minute bucket (cache of normal_candidates table).
        self._pending_normal: dict[str, int] = {}

    # ------------------------------------------------------------- lifecycle

    def recover(self) -> None:
        """Rebuild in-memory state from persisted rows."""
        with self._lock:
            self._open.clear()
            self._pending_normal.clear()
            for row in self.storage.list_open_traces():
                self._open[row["trace_id"]] = row["last_span_ns"]
            for c in self.storage.list_candidates():
                self._pending_normal[c["trace_id"]] = c["minute_bucket"]
            # Repair traces closed but never decided (e.g. hard crash).
            self._recover_orphans()
            self.storage.commit()

    def _recover_orphans(self) -> None:
        rows = self.storage.conn.execute(
            "SELECT trace_id FROM traces WHERE state = 'closed_pending'"
        ).fetchall()
        for row in rows:
            tid = row["trace_id"]
            if tid in self._pending_normal:
                continue
            if self.storage.get_decision(tid) is not None:
                continue
            trace = self.storage.get_trace(tid)
            if trace is None:
                continue
            agg = self.storage.recompute_trace(tid)
            if agg is None:
                continue
            bucket = minute_bucket(agg["end_ns"])
            is_slow = (agg["end_ns"] - agg["start_ns"]) >= self.settings.slow_threshold_ns
            if sampler.classify(agg["is_error"], is_slow):
                self._decide_priority(tid, bucket, agg, is_slow)
            else:
                self.storage.insert_candidate(
                    tid, bucket, trace["closed_ns"] or now_ns()
                )
                self._pending_normal[tid] = bucket

    # ---------------------------------------------------------------- ingest

    def ingest(self, spans: list[SpanIn]) -> dict[str, Any]:
        accepted = rejected = duplicates = 0
        touched: list[str] = []
        received = now_ns()
        with self._lock:
            for span in spans:
                tid = span.trace_id
                try:
                    start_ns = to_ns(span.start_time)
                    end_ns = to_ns(span.end_time)
                except (ValueError, OverflowError, TypeError):
                    rejected += 1
                    continue
                if end_ns < start_ns:
                    rejected += 1
                    continue

                existing = self.storage.get_trace(tid)
                if existing is not None and existing["state"] != "open":
                    # Trace already closed: decisions are immutable.
                    rejected += 1
                    continue

                inserted = self.storage.insert_span(
                    trace_id=tid,
                    span_id=span.span_id,
                    parent_span_id=span.parent_span_id,
                    name=span.name,
                    service=span.service,
                    start_ns=start_ns,
                    end_ns=end_ns,
                    status=span.status,
                    attributes=span.attributes,
                    received_ns=received,
                )
                if not inserted:
                    duplicates += 1
                    continue

                is_error = span.status == "error" or _attrs_signal_error(
                    span.attributes
                )
                ok = self.storage.upsert_open_trace(
                    trace_id=tid,
                    start_ns=start_ns,
                    end_ns=end_ns,
                    last_span_ns=received,
                    is_error=is_error,
                    now_ns=received,
                )
                if not ok:
                    # State flipped between the check and upsert (unreachable
                    # while the engine lock is held, kept defensively): remove
                    # the orphan span and reject instead of rolling back the
                    # whole batch.
                    self.storage.conn.execute(
                        "DELETE FROM spans WHERE trace_id = ? AND span_id = ?",
                        (tid, span.span_id),
                    )
                    rejected += 1
                    continue

                self._open[tid] = received
                accepted += 1
                touched.append(tid)
            self.storage.commit()

            traces: dict[str, Any] = {}
            for tid in dict.fromkeys(touched):
                traces[tid] = self._view(tid)
            return {
                "accepted": accepted,
                "rejected": rejected,
                "duplicates": duplicates,
                "traces": traces,
            }

    # -------------------------------------------------------------- deciding

    def _decide_priority(
        self, tid: str, bucket: int, agg: dict[str, Any], is_slow: bool,
    ) -> dict[str, Any]:
        reason = sampler.classify(agg["is_error"], is_slow)
        assert reason is not None
        self.storage.ensure_minute(bucket)
        counter = self.storage.get_counter(bucket)
        used = counter["priority_used"] + counter["normal_kept"]
        if used >= self.settings.sample_budget_per_minute:
            decision, reason_out = "drop", sampler.REASON_BUDGET_EXHAUSTED
        else:
            decision, reason_out = "keep", reason
            self.storage.add_priority_used(bucket, 1)
        d = self._write_decision(tid, bucket, agg, is_slow, decision, reason_out)
        self.storage.set_trace_state(
            tid, "priority_kept" if decision == "keep" else "dropped"
        )
        return d

    def _write_decision(
        self, tid: str, bucket: int, agg: dict[str, Any], is_slow: bool,
        decision: str, reason: str,
    ) -> dict[str, Any]:
        d = {
            "trace_id": tid,
            "decision": decision,
            "reason": reason,
            "is_error": bool(agg["is_error"]),
            "is_slow": is_slow,
            "duration_ns": agg["end_ns"] - agg["start_ns"],
            "span_count": agg["span_count"],
            "minute_bucket": bucket,
            "decided_at_ns": now_ns(),
        }
        self.storage.upsert_decision(d)
        return d

    # ------------------------------------------------------------- scheduler

    def tick(self) -> None:
        """One aggregation pass: close idle traces and settle due buckets."""
        with self._lock:
            self._close_idle_traces()
            self._flush_due_buckets(force=False)
            self.storage.commit()

    def force_flush(self, close_open: bool = True) -> dict[str, int]:
        """Admin/demo helper: close everything pending and settle all buckets."""
        with self._lock:
            if close_open:
                self._close_idle_traces(idle_grace_ns=0)
            closed_buckets = self._flush_due_buckets(force=True)
            self.storage.commit()
            return {"buckets_flushed": closed_buckets}

    def _close_idle_traces(self, idle_grace_ns: int | None = None) -> None:
        if idle_grace_ns is None:
            idle_grace_ns = int(self.settings.idle_window_seconds * 1_000_000_000)
        current = now_ns()
        idle = [
            tid for tid, last in list(self._open.items())
            if current - last >= idle_grace_ns
        ]
        for tid in idle:
            self._close_trace(tid, closed_ns=current)

    def _close_trace(self, tid: str, closed_ns: int) -> None:
        agg = self.storage.recompute_trace(tid)
        if agg is None:
            self._open.pop(tid, None)
            return
        bucket = minute_bucket(agg["end_ns"])
        if not self.storage.close_trace(tid, bucket, closed_ns, agg):
            self._open.pop(tid, None)
            return
        self._open.pop(tid, None)
        duration = agg["end_ns"] - agg["start_ns"]
        is_slow = duration >= self.settings.slow_threshold_ns
        if sampler.classify(agg["is_error"], is_slow):
            self._decide_priority(tid, bucket, agg, is_slow)
        else:
            self.storage.insert_candidate(tid, bucket, closed_ns)
            self._pending_normal[tid] = bucket

    def _bucket_deadline_ns(self, bucket: int) -> int:
        # Minute end + one silence window (+ one tick): by then every priority
        # trace whose event time belongs to this bucket has closed.
        return (
            (bucket + 60) * 1_000_000_000
            + int(self.settings.idle_window_seconds * 1_000_000_000)
            + int(self.settings.tick_interval_seconds * 1_000_000_000)
        )

    def _flush_due_buckets(self, force: bool) -> int:
        current = now_ns()
        cur_bucket = minute_bucket(current)
        due: set[int] = set()
        for c in self.storage.list_candidates():
            b = c["minute_bucket"]
            if force or (b < cur_bucket and current >= self._bucket_deadline_ns(b)):
                due.add(b)
        flushed = 0
        for bucket in sorted(due):
            self._flush_bucket(bucket)
            flushed += 1
        return flushed

    def _flush_bucket(self, bucket: int) -> None:
        candidates = self.storage.candidates_for_bucket(bucket)
        if not candidates:
            return
        self.storage.ensure_minute(bucket)
        counter = self.storage.get_counter(bucket)
        used = counter["priority_used"] + counter["normal_kept"]
        remaining = max(0, self.settings.sample_budget_per_minute - used)

        ids = [c["trace_id"] for c in candidates]
        kept, dropped = sampler.select_by_hash(ids, remaining)
        kept_set = set(kept)

        for c in candidates:
            tid = c["trace_id"]
            trace = self.storage.get_trace(tid)
            agg = self.storage.recompute_trace(tid)
            if agg is None:
                continue
            is_slow = (
                agg["end_ns"] - agg["start_ns"]
            ) >= self.settings.slow_threshold_ns
            if tid in kept_set:
                decision, reason = "keep", sampler.REASON_HASH_SELECTED
            elif remaining == 0:
                decision, reason = "drop", sampler.REASON_NORMAL_NO_BUDGET
            else:
                decision, reason = "drop", sampler.REASON_HASH_NOT_SELECTED
            self._write_decision(tid, bucket, agg, is_slow, decision, reason)
            self.storage.set_trace_state(
                tid, "normal_kept" if decision == "keep" else "dropped"
            )
            self._pending_normal.pop(tid, None)

        self.storage.delete_candidates(ids)
        self.storage.conn.execute(
            "UPDATE minute_counters SET normal_kept = normal_kept + ?, "
            "normal_dropped = normal_dropped + ?, flushed = 1 "
            "WHERE minute_bucket = ?",
            (len(kept), len(dropped), bucket),
        )

    # ---------------------------------------------------------------- queries

    def _view(self, tid: str) -> dict[str, Any]:
        trace = self.storage.get_trace(tid)
        if trace is None:
            return {
                "trace_id": tid,
                "state": "unknown",
                "decision": "drop",
                "reason": sampler.REASON_UNKNOWN,
            }
        decision = self.storage.get_decision(tid)
        if decision is None:
            return {
                "trace_id": tid,
                "state": "open" if trace["state"] == "open" else "pending",
                "decision": "pending",
                "reason": sampler.REASON_PENDING,
                "span_count": trace["span_count"],
            }
        state = self._state_from_decision(decision)
        return {
            "trace_id": tid,
            "state": state,
            "decision": decision["decision"],
            "reason": decision["reason"],
            "is_error": bool(decision["is_error"]),
            "is_slow": bool(decision["is_slow"]),
            "duration_ns": decision["duration_ns"],
            "span_count": decision["span_count"],
            "minute_bucket": decision["minute_bucket"],
            "decided_at_ns": decision["decided_at_ns"],
        }

    @staticmethod
    def _state_from_decision(decision: dict[str, Any]) -> str:
        if decision["decision"] == "drop":
            return "dropped"
        if decision["reason"] in (sampler.REASON_ERROR, sampler.REASON_SLOW):
            return "priority_kept"
        return "normal_kept"

    def get_decision(self, tid: str) -> dict[str, Any] | None:
        with self._lock:
            if self.storage.get_trace(tid) is None:
                return None
            return self._view(tid)

    def get_decisions(self, trace_ids: list[str]) -> tuple[dict[str, Any], list[str]]:
        with self._lock:
            known = self.storage.get_traces(trace_ids)
            views = {tid: self._view(tid) for tid in known}
            missing = [tid for tid in trace_ids if tid not in known]
            return views, missing

    def get_service_time(self, tid: str) -> dict[str, Any] | None:
        """Exclusive time per service from one consistent snapshot.

        Works for decided and open traces alike; for open traces the snapshot
        spans exactly what has been received at query time (the engine lock
        serializes against ingest). Returns None for unknown traces.
        """
        with self._lock:
            if self.storage.get_trace(tid) is None:
                return None
            rows = list(self.storage.iter_span_rows(tid))
            return {
                "trace_id": tid,
                "span_count": len(rows),
                "services": servicetime.exclusive_by_service(rows),
            }

    def get_concurrency(
        self, tid: str, min_active: int
    ) -> dict[str, Any] | None:
        """Windows with >= ``min_active`` spans active, from one snapshot.

        Works for decided and open traces alike; for open traces the snapshot
        spans exactly what has been received at query time (the engine lock
        serializes against ingest). Returns None for unknown traces.
        """
        with self._lock:
            if self.storage.get_trace(tid) is None:
                return None
            rows = list(self.storage.iter_span_rows(tid))
            windows = concurrency.active_windows(rows, min_active)
            return {
                "trace_id": tid,
                "min_active": min_active,
                "span_count": len(rows),
                "window_count": len(windows),
                "total_concurrent_ns": sum(w["duration_ns"] for w in windows),
                "windows": windows,
            }

    def get_integrity(self, tid: str) -> dict[str, Any] | None:
        """Reference and time-boundary checks from one consistent snapshot.

        Works for decided and open traces alike; for open traces the snapshot
        spans exactly what has been received at query time (the engine lock
        serializes against ingest). Returns None for unknown traces.
        """
        with self._lock:
            if self.storage.get_trace(tid) is None:
                return None
            rows = list(self.storage.iter_span_rows(tid))
            anomalies = integrity.check_integrity(rows)
            return {
                "trace_id": tid,
                "span_count": len(rows),
                "anomaly_count": len(anomalies),
                "valid": not anomalies,
                "anomalies": anomalies,
            }

    def get_service_edges(self, tid: str) -> dict[str, Any] | None:
        """Cross-service direct call edges from one consistent snapshot.

        Works for decided and open traces alike; for open traces the snapshot
        spans exactly what has been received at query time (the engine lock
        serializes against ingest). Returns None for unknown traces.
        """
        with self._lock:
            if self.storage.get_trace(tid) is None:
                return None
            rows = list(self.storage.iter_span_rows(tid))
            edges = serviceedges.service_edges(rows)
            return {
                "trace_id": tid,
                "span_count": len(rows),
                "edge_count": len(edges),
                "edges": edges,
            }

    def get_dominant_path(self, tid: str) -> dict[str, Any] | None:
        """Heaviest root-to-leaf call chain from one consistent snapshot.

        Works for decided and open traces alike; for open traces the snapshot
        spans exactly what has been received at query time (the engine lock
        serializes against ingest). Returns None for unknown traces. Raises
        dominantpath.CyclicTraceError when self references or parent cycles
        make root-to-leaf paths undefined.
        """
        with self._lock:
            if self.storage.get_trace(tid) is None:
                return None
            rows = list(self.storage.iter_span_rows(tid))
            result = dominantpath.dominant_path(rows)
            return {
                "trace_id": tid,
                "span_count": len(rows),
                **result,
            }

    def stats(self) -> dict[str, Any]:
        with self._lock:
            retained = self.storage.conn.execute(
                "SELECT COUNT(*) AS c FROM decisions WHERE decision = 'keep'"
            ).fetchone()["c"]
            dropped_total = self.storage.conn.execute(
                "SELECT COUNT(*) AS c FROM decisions WHERE decision = 'drop'"
            ).fetchone()["c"]
            budget = {}
            for row in self.storage.list_counters():
                budget[str(row["minute_bucket"])] = {
                    "priority_used": row["priority_used"],
                    "normal_kept": row["normal_kept"],
                    "normal_dropped": row["normal_dropped"],
                    "retained": row["priority_used"] + row["normal_kept"],
                    "budget": self.settings.sample_budget_per_minute,
                }
            return {
                "open_traces": len(self._open),
                "pending_normal": len(self._pending_normal),
                "decided_traces": self.storage.count_decisions(),
                "retained": retained,
                "dropped": dropped_total,
                "budget": budget,
            }
