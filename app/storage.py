"""SQLite persistence layer.

The storage class is intentionally free of sampling logic: the engine owns
policy, this module owns SQL. Every method is synchronous; the engine
serializes calls with a lock and SQLite is opened with a single connection.
"""
from __future__ import annotations

import json
import os
import sqlite3
from typing import Any, Iterable

SCHEMA = """
CREATE TABLE IF NOT EXISTS spans (
    trace_id      TEXT NOT NULL,
    span_id       TEXT NOT NULL,
    parent_span_id TEXT,
    name          TEXT,
    service       TEXT,
    start_ns      INTEGER NOT NULL,
    end_ns        INTEGER NOT NULL,
    status        TEXT NOT NULL,
    is_error      INTEGER NOT NULL DEFAULT 0,
    attributes_json TEXT NOT NULL DEFAULT '{}',
    received_ns   INTEGER NOT NULL,
    PRIMARY KEY (trace_id, span_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS traces (
    trace_id     TEXT PRIMARY KEY,
    start_ns     INTEGER NOT NULL,
    end_ns       INTEGER NOT NULL,
    last_span_ns INTEGER NOT NULL,
    span_count   INTEGER NOT NULL,
    is_error     INTEGER NOT NULL DEFAULT 0,
    state        TEXT NOT NULL,
    closed_ns    INTEGER,
    minute_bucket INTEGER,
    updated_ns   INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS normal_candidates (
    trace_id      TEXT PRIMARY KEY,
    minute_bucket INTEGER NOT NULL,
    closed_ns      INTEGER NOT NULL
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_candidates_bucket
    ON normal_candidates(minute_bucket);

CREATE TABLE IF NOT EXISTS decisions (
    trace_id      TEXT PRIMARY KEY,
    decision      TEXT NOT NULL,
    reason        TEXT NOT NULL,
    is_error      INTEGER NOT NULL,
    is_slow       INTEGER NOT NULL,
    duration_ns   INTEGER NOT NULL,
    span_count    INTEGER NOT NULL,
    minute_bucket INTEGER NOT NULL,
    decided_at_ns INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS minute_counters (
    minute_bucket  INTEGER PRIMARY KEY,
    priority_used  INTEGER NOT NULL DEFAULT 0,
    normal_kept    INTEGER NOT NULL DEFAULT 0,
    normal_dropped INTEGER NOT NULL DEFAULT 0,
    flushed        INTEGER NOT NULL DEFAULT 0
) WITHOUT ROWID;
"""

# Partial indexes serving the cross-trace error-span search, pre-ordered by
# start_ns DESC, trace_id ASC, span_id ASC. SQLite >= 3.43 serves the whole
# ORDER BY straight from the index; older versions ignore the DESC direction
# but still use the index for filtering and only sort start-time ties.
SPAN_ERROR_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_spans_error_start
    ON spans(start_ns DESC, trace_id ASC, span_id ASC) WHERE is_error = 1;

CREATE INDEX IF NOT EXISTS idx_spans_error_service_start
    ON spans(service ASC, start_ns DESC, trace_id ASC, span_id ASC)
    WHERE is_error = 1;
"""


class Storage:
    def __init__(self, path: str) -> None:
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA)
        self._migrate_spans_error_column()
        self.conn.executescript(SPAN_ERROR_INDEXES)
        self.conn.commit()

    def close(self) -> None:
        self.conn.commit()
        self.conn.close()

    def _migrate_spans_error_column(self) -> None:
        """Add the per-span error flag to databases created before it existed.

        Rows are backfilled with the same status/attribute predicate used at
        ingest time, so historical spans follow the identical error policy.
        """
        cols = {
            r["name"]
            for r in self.conn.execute("PRAGMA table_info(spans)").fetchall()
        }
        if not cols or "is_error" in cols:
            return
        self.conn.execute(
            "ALTER TABLE spans ADD COLUMN is_error INTEGER NOT NULL DEFAULT 0"
        )
        rows = self.conn.execute(
            "SELECT trace_id, span_id, status, attributes_json FROM spans"
        ).fetchall()
        for r in rows:
            is_error = r["status"] == "error" or _attrs_signal_error(
                json.loads(r["attributes_json"] or "{}")
            )
            if is_error:
                self.conn.execute(
                    "UPDATE spans SET is_error = 1 "
                    "WHERE trace_id = ? AND span_id = ?",
                    (r["trace_id"], r["span_id"]),
                )

    # ------------------------------------------------------------------ spans

    def insert_span(
        self,
        *,
        trace_id: str,
        span_id: str,
        parent_span_id: str | None,
        name: str | None,
        service: str | None,
        start_ns: int,
        end_ns: int,
        status: str,
        is_error: bool,
        attributes: dict[str, Any],
        received_ns: int,
    ) -> bool:
        """Insert a span. Returns False on (trace_id, span_id) duplicate."""
        cur = self.conn.execute(
            """
            INSERT OR IGNORE INTO spans
                (trace_id, span_id, parent_span_id, name, service,
                 start_ns, end_ns, status, is_error, attributes_json, received_ns)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (trace_id, span_id, parent_span_id, name, service, start_ns,
             end_ns, status, int(is_error),
             json.dumps(attributes, separators=(",", ":")),
             received_ns),
        )
        return cur.rowcount == 1

    def iter_span_rows(self, trace_id: str) -> Iterable[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM spans WHERE trace_id = ? ORDER BY start_ns",
            (trace_id,),
        )

    def find_error_spans(
        self,
        *,
        range_start_ns: int,
        range_end_ns: int,
        service: str | None,
        limit: int,
        cursor: tuple[int, str, str] | None,
    ) -> list[sqlite3.Row]:
        """Return up to ``limit + 1`` error spans ordered for key-set paging.

        Ordering is ``start_ns DESC, trace_id ASC, span_id ASC`` over spans
        whose start time lies in the half-open window
        ``[range_start_ns, range_end_ns)``. ``cursor`` is the
        ``(start_ns, trace_id, span_id)`` of the last previously returned row;
        the extra row (when present) signals that another page exists.
        """
        where = [
            "is_error = 1",
            "start_ns >= ?",
            "start_ns < ?",
        ]
        params: list[Any] = [range_start_ns, range_end_ns]
        if service is not None:
            where.append("service = ?")
            params.append(service)
        if cursor is not None:
            # start_ns < cursor start, or same start with a strictly greater
            # (trace_id, span_id) in ascending order.
            where.append(
                "(start_ns < ? OR (start_ns = ? AND "
                "(trace_id > ? OR (trace_id = ? AND span_id > ?))))"
            )
            c_start, c_trace, c_span = cursor
            params.extend([c_start, c_start, c_trace, c_trace, c_span])

        sql = (
            "SELECT trace_id, span_id, service, start_ns, end_ns, status "
            "FROM spans WHERE " + " AND ".join(where) + " "
            "ORDER BY start_ns DESC, trace_id ASC, span_id ASC "
            "LIMIT ?"
        )
        params.append(limit + 1)
        return self.conn.execute(sql, params).fetchall()

    # ----------------------------------------------------------------- traces

    def upsert_open_trace(
        self,
        *,
        trace_id: str,
        start_ns: int,
        end_ns: int,
        last_span_ns: int,
        is_error: bool,
        now_ns: int,
    ) -> bool:
        """Insert or extend an *open* trace aggregate.

        Returns False when the trace is already in a terminal state (a late
        span for a decided trace — it must not reopen the trace).
        """
        cur = self.conn.execute(
            """
            INSERT INTO traces
                (trace_id, start_ns, end_ns, last_span_ns, span_count,
                 is_error, state, updated_ns)
            VALUES (?, ?, ?, ?, 1, ?, 'open', ?)
            ON CONFLICT(trace_id) DO UPDATE SET
                start_ns     = MIN(traces.start_ns, excluded.start_ns),
                end_ns       = MAX(traces.end_ns, excluded.end_ns),
                last_span_ns = MAX(traces.last_span_ns, excluded.last_span_ns),
                span_count   = traces.span_count + 1,
                is_error     = MAX(traces.is_error, excluded.is_error),
                updated_ns   = excluded.updated_ns
            WHERE traces.state = 'open'
            """,
            (trace_id, start_ns, end_ns, last_span_ns, int(is_error), now_ns),
        )
        return cur.rowcount == 1

    def get_trace(self, trace_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM traces WHERE trace_id = ?", (trace_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_traces(self, trace_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not trace_ids:
            return {}
        marks = ",".join("?" for _ in trace_ids)
        rows = self.conn.execute(
            f"SELECT * FROM traces WHERE trace_id IN ({marks})", trace_ids
        ).fetchall()
        return {r["trace_id"]: dict(r) for r in rows}

    def list_open_traces(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM traces WHERE state = 'open'"
        ).fetchall()
        return [dict(r) for r in rows]

    def recompute_trace(self, trace_id: str) -> dict[str, Any] | None:
        """Rebuild aggregates directly from the span table."""
        rows = list(self.iter_span_rows(trace_id))
        if not rows:
            return None
        is_error = False
        for r in rows:
            if r["status"] == "error":
                is_error = True
                break
            attrs = json.loads(r["attributes_json"] or "{}")
            if _attrs_signal_error(attrs):
                is_error = True
                break
        return {
            "trace_id": trace_id,
            "start_ns": min(r["start_ns"] for r in rows),
            "end_ns": max(r["end_ns"] for r in rows),
            "last_span_ns": max(r["received_ns"] for r in rows),
            "span_count": len(rows),
            "is_error": is_error,
        }

    def close_trace(
        self, trace_id: str, minute_bucket: int, closed_ns: int,
        agg: dict[str, Any],
    ) -> bool:
        """Move an open trace into a terminal-pending state."""
        cur = self.conn.execute(
            """
            UPDATE traces SET
                state = 'closed_pending',
                closed_ns = ?,
                minute_bucket = ?,
                start_ns = ?,
                end_ns = ?,
                last_span_ns = ?,
                span_count = ?,
                is_error = ?
            WHERE trace_id = ? AND state = 'open'
            """,
            (closed_ns, minute_bucket, agg["start_ns"], agg["end_ns"],
             agg["last_span_ns"], agg["span_count"], int(agg["is_error"]),
             trace_id),
        )
        return cur.rowcount == 1

    def set_trace_state(self, trace_id: str, state: str) -> None:
        self.conn.execute(
            "UPDATE traces SET state = ? WHERE trace_id = ?",
            (state, trace_id),
        )

    # ------------------------------------------------------- normal candidate

    def insert_candidate(
        self, trace_id: str, minute_bucket: int, closed_ns: int
    ) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO normal_candidates
                (trace_id, minute_bucket, closed_ns)
            VALUES (?, ?, ?)
            """,
            (trace_id, minute_bucket, closed_ns),
        )

    def delete_candidates(self, trace_ids: Iterable[str]) -> None:
        ids = list(trace_ids)
        if not ids:
            return
        marks = ",".join("?" for _ in ids)
        self.conn.execute(
            f"DELETE FROM normal_candidates WHERE trace_id IN ({marks})", ids
        )

    def list_candidates(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM normal_candidates ORDER BY closed_ns"
        ).fetchall()
        return [dict(r) for r in rows]

    def candidates_for_bucket(self, minute_bucket: int) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT * FROM normal_candidates
            WHERE minute_bucket = ?
            ORDER BY closed_ns, trace_id
            """,
            (minute_bucket,),
        ).fetchall()
        return [dict(r) for r in rows]

    # -------------------------------------------------------------- decisions

    def get_decision(self, trace_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM decisions WHERE trace_id = ?", (trace_id,)
        ).fetchone()
        return dict(row) if row else None

    def upsert_decision(self, d: dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO decisions
                (trace_id, decision, reason, is_error, is_slow, duration_ns,
                 span_count, minute_bucket, decided_at_ns)
            VALUES (:trace_id, :decision, :reason, :is_error, :is_slow,
                    :duration_ns, :span_count, :minute_bucket, :decided_at_ns)
            """,
            {
                "trace_id": d["trace_id"],
                "decision": d["decision"],
                "reason": d["reason"],
                "is_error": int(d["is_error"]),
                "is_slow": int(d["is_slow"]),
                "duration_ns": d["duration_ns"],
                "span_count": d["span_count"],
                "minute_bucket": d["minute_bucket"],
                "decided_at_ns": d["decided_at_ns"],
            },
        )

    def count_decisions(self) -> int:
        return self.conn.execute(
            "SELECT COUNT(*) AS c FROM decisions"
        ).fetchone()["c"]

    # ------------------------------------------------------------ minute stats

    def ensure_minute(self, bucket: int) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO minute_counters (minute_bucket) VALUES (?)",
            (bucket,),
        )

    def add_priority_used(self, bucket: int, n: int = 1) -> None:
        self.ensure_minute(bucket)
        self.conn.execute(
            "UPDATE minute_counters SET priority_used = priority_used + ? "
            "WHERE minute_bucket = ?",
            (n, bucket),
        )

    def get_counter(self, bucket: int) -> dict[str, int] | None:
        row = self.conn.execute(
            "SELECT * FROM minute_counters WHERE minute_bucket = ?", (bucket,)
        ).fetchone()
        return dict(row) if row else None

    def list_counters(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM minute_counters ORDER BY minute_bucket DESC LIMIT 120"
        ).fetchall()
        return [dict(r) for r in rows]

    def commit(self) -> None:
        self.conn.commit()


def _attrs_signal_error(attrs: dict[str, Any]) -> bool:
    """Recognize conventional error attributes (OpenTelemetry-ish)."""
    if attrs.get("error") is True:
        return True
    if str(attrs.get("error", "")).lower() == "true":
        return True
    if "exception.type" in attrs or "exception" in attrs:
        return True
    status_code = attrs.get("http.status_code")
    if isinstance(status_code, bool):
        return False
    if isinstance(status_code, str) and status_code.isdigit():
        status_code = int(status_code)
    if isinstance(status_code, int) and 500 <= status_code < 600:
        return True
    return False
