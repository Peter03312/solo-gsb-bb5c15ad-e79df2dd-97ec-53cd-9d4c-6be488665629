# Tail Sampling Service

A self-contained **distributed-tracing tail sampler** built with Python +
FastAPI. Clients submit spans in batches; the service aggregates them into
complete traces, waits for a silence window, and decides **keep / drop** using:

1. **Priority rules (evaluated first)**
   * any span in the trace has `status: "error"` (or a conventional error
     attribute such as `http.status_code: 5xx`, `error: true`,
     `exception.type`);
   * the trace end-to-end duration exceeds the slow-trace threshold.
2. **Per-minute sampling budget** — at most `N` retained traces per minute.
   Priority traces take slots first; when the minute settles, ordinary traces
   fill the remaining slots with **stable-hash ranking** (BLAKE2b over the
   trace id), so the same trace is always selected on every process/restart
   and selection spreads uniformly over trace ids.

Every response carries the decision (`keep` / `drop` / `pending`) and the
**reason** it was hit (`error_trace`, `slow_trace`,
`priority_budget_exhausted`, `stable_hash_selected`,
`stable_hash_not_selected`, `normal_budget_exhausted`).

State lives in a single SQLite file (WAL mode). No third-party accounts, no
external services.

---

## Quick start (Docker Compose)

```bash
docker compose up --build
```

| What | Where |
| --- | --- |
| Service / API base | <http://localhost:8080> |
| Interactive API docs (Swagger) | <http://localhost:8080/docs> |
| Health check | `GET http://localhost:8080/health` |

The SQLite database is kept in the named volume `sampler-data`
(container path `/data/traces.db`) and survives `docker compose down`
(use `docker compose down -v` to wipe it).

Stop:

```bash
docker compose down
```

### Run without Docker

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
TS_DATABASE_PATH=./data/traces.db \
  uvicorn app.main:app --host 0.0.0.0 --port 8080
```

---

## Try it (no external tools needed)

With the service running, use the zero-dependency demo client (it sends a mix
of normal/error/slow traces and prints the decisions):

```bash
python3 scripts/demo_client.py http://localhost:8080
```

Or with curl:

```bash
curl -s -X POST http://localhost:8080/api/v1/spans:batch \
  -H 'Content-Type: application/json' \
  -d '{
    "spans": [
      {
        "trace_id": "abc-123",
        "span_id": "root",
        "service": "checkout",
        "name": "POST /order",
        "start_time": "2026-09-18T10:00:00Z",
        "end_time":   "2026-09-18T10:00:02.5Z",
        "status": "ok",
        "attributes": {"http.status_code": 200}
      }
    ]
  }'

# wait the silence window (default 10s) ...
curl -s http://localhost:8080/api/v1/decisions/abc-123
```

Decision lifecycle for a trace:

```
open  ──(silence window elapsed)──►  pending (normal traces, until minute settles)
open  ────────────────────────────► priority_kept / dropped (error|slow decided at close)
pending ──(minute end + silence window)──► normal_kept / dropped
```

For tests/demos you can skip the waiting with
`POST /api/v1/admin/force-flush` (closes all open traces and settles all
minutes immediately).

---

## API

All endpoints are JSON and documented at `/docs`.

### `POST /api/v1/spans:batch` — submit a batch of spans

```json
{
  "spans": [
    {
      "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
      "span_id": "00f067aa0ba902b7",
      "parent_span_id": null,
      "name": "GET /cart",
      "service": "shop-web",
      "start_time": "2026-09-18T10:00:00.100Z",
      "end_time":   "2026-09-18T10:00:00.320Z",
      "status": "ok",
      "attributes": {"http.status_code": 200, "user_id": "u-42"}
    }
  ]
}
```

* `start_time` / `end_time` accept ISO-8601 strings (`...Z` or with offset)
  **or** epoch numbers in seconds, milliseconds, microseconds or nanoseconds
  (unit is auto-detected).
* `status`: `ok` | `error` | `unset` (case-insensitive).
* `(trace_id, span_id)` duplicates are idempotent and reported as
  `duplicates`; spans arriving after the trace was decided are `rejected`.

Response:

```json
{
  "accepted": 1,
  "rejected": 0,
  "duplicates": 0,
  "traces": {
    "4bf92f3577b34da6a3ce929d0e0e4736": {
      "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
      "state": "open",
      "decision": "pending",
      "reason": "trace_aggregating",
      "span_count": 1
    }
  }
}
```

### `GET /api/v1/decisions/{trace_id}` — query one decision

```json
{
  "trace_id": "abc-123",
  "state": "priority_kept",
  "decision": "keep",
  "reason": "slow_trace",
  "is_error": false,
  "is_slow": true,
  "duration_ns": 2500000000,
  "span_count": 1,
  "minute_bucket": 1789716000,
  "decided_at_ns": 1789716022123000000
}
```

Unknown trace id → `404`.

### `POST /api/v1/decisions:batch` — query many decisions

```json
{ "trace_ids": ["abc-123", "def-456"] }
```

Returns `{ "decisions": { ... }, "missing": [ ... ] }`.

### `GET /api/v1/traces/{trace_id}/service-time` — exclusive time per service

Breaks one trace down by service so you can see where the time actually went.
Each span's **exclusive** time is its own `[start, end)` length minus the time
covered by its direct children (overlapping children are subtracted once,
self-referencing spans are ignored, zero-length spans count as 0). Spans
without a `service` attribute are grouped under `null`. Works on open traces
too — they are computed from a consistent snapshot of the spans received so
far.

```bash
curl -s http://localhost:8080/api/v1/traces/abc-123/service-time
```

```json
{
  "trace_id": "abc-123",
  "span_count": 4,
  "services": [
    { "service": "checkout",     "span_count": 1, "exclusive_ns": 1100000000 },
    { "service": "payments",     "span_count": 1, "exclusive_ns": 500000000 },
    { "service": "payments-db",  "span_count": 1, "exclusive_ns": 500000000 },
    { "service": null,           "span_count": 1, "exclusive_ns": 400000000 }
  ]
}
```

`services` is sorted by `exclusive_ns` descending, then service name
ascending with the `null` group last. Unknown trace id → `404`.

### `GET /api/v1/traces/{trace_id}/concurrency?min_active=2` — concurrent-span windows

Finds the time windows inside one trace where at least `min_active` spans
were running at the same time. The stored nanosecond `[start, end)` span
intervals are swept for the active-span count; intervals are half-open, so a
span ending exactly where another starts does **not** create a false overlap,
stretches that stay at or above the threshold merge into one continuous
window, and zero-length spans never count. Works on open traces too — they
are computed from a consistent snapshot of the spans received so far.

```bash
curl -s "http://localhost:8080/api/v1/traces/abc-123/concurrency?min_active=2"
```

```json
{
  "trace_id": "abc-123",
  "min_active": 2,
  "span_count": 4,
  "window_count": 1,
  "total_concurrent_ns": 40000000000,
  "windows": [
    {
      "start_ns": 1780000040000000000,
      "end_ns": 1780000080000000000,
      "duration_ns": 40000000000,
      "peak_active": 2
    }
  ]
}
```

* `windows` — merged qualifying intervals, sorted by start, each with its
  `peak_active` (highest simultaneous span count inside the window).
* `total_concurrent_ns` — total time at or above the threshold.
* `min_active` defaults to `2`; values below 2 → `422`. Unknown trace id →
  `404`.

### `GET /api/v1/traces/{trace_id}/integrity` — reference & time-boundary checks

Validates the parent/child graph of the spans received so far and the time
containment of every child within its parent. It reports four kinds of
anomaly:

| `type` | Meaning |
| --- | --- |
| `missing_parent` | A span's `parent_span_id` names a span the trace never received |
| `self_reference` | A span lists itself as its own parent |
| `cycle` | Two or more spans reference each other in a ring; `span_ids` lists every ring member (each ring is reported **once**) |
| `time_out_of_bounds` | Parent exists and the edge is not part of a cycle, but the child's `[start, end)` is not inside the parent's interval |

Cycle edges are excluded from the time check (a ring has no meaningful
containing interval). Interval bounds are inclusive, so a child touching its
parent's start/end is valid; `start_overflow_ns` / `end_overflow_ns` give the
amount by which the child sticks out on each side. Works on open traces too —
computed from a consistent snapshot of the spans received so far.

```bash
curl -s http://localhost:8080/api/v1/traces/abc-123/integrity
```

Valid snapshot:

```json
{
  "trace_id": "abc-123",
  "span_count": 3,
  "anomaly_count": 0,
  "valid": true,
  "anomalies": []
}
```

Snapshot with problems:

```json
{
  "trace_id": "abc-123",
  "span_count": 5,
  "anomaly_count": 3,
  "valid": false,
  "anomalies": [
    {
      "type": "missing_parent",
      "span_ids": ["orphan"],
      "parent_span_id": "ghost",
      "message": "span 'orphan' references unknown parent span 'ghost'",
      "child_start_ns": null,
      "child_end_ns": null,
      "parent_start_ns": null,
      "parent_end_ns": null,
      "start_overflow_ns": null,
      "end_overflow_ns": null
    },
    {
      "type": "self_reference",
      "span_ids": ["self"],
      "parent_span_id": "self",
      "message": "span 'self' references itself as its parent",
      "child_start_ns": null,
      "child_end_ns": null,
      "parent_start_ns": null,
      "parent_end_ns": null,
      "start_overflow_ns": null,
      "end_overflow_ns": null
    },
    {
      "type": "time_out_of_bounds",
      "span_ids": ["child", "root"],
      "parent_span_id": "root",
      "message": "span 'child' ends after its parent span 'root'",
      "child_start_ns": 1780000000000000000,
      "child_end_ns": 1780000090000000000,
      "parent_start_ns": 1780000000000000000,
      "parent_end_ns": 1780000080000000000,
      "start_overflow_ns": 0,
      "end_overflow_ns": 10000000000
    }
  ]
}
```

`anomalies` are ordered by type (`missing_parent`, `self_reference`,
`cycle`, `time_out_of_bounds`) and then by the spans involved; cycle member
lists are rotated to start at the smallest span id. Unknown trace id →
`404`.

### `GET /api/v1/traces/{trace_id}/service-edges` — cross-service call edges

Builds the service-level call graph of one trace from the spans received so
far. A parent→child pair counts as an edge only when the **parent span has
arrived**, both spans carry a non-empty `service`, and the two services
**differ**; qualifying pairs are aggregated by
`(source_service, target_service)` with a call count. `duration_ns` is
computed per parent span and target service: each direct child's
`[start, end)` interval is intersected with the parent's interval, the
intersections are unioned (overlapping children count once), and the
per-parent unions are summed across parent spans. Missing parents, self
references and zero-length intersections contribute no duration (a zero-length
child still counts as a call). Works on open traces too — computed from a
consistent snapshot of the spans received so far.

```bash
curl -s http://localhost:8080/api/v1/traces/abc-123/service-edges
```

```json
{
  "trace_id": "abc-123",
  "span_count": 4,
  "edge_count": 2,
  "edges": [
    { "source_service": "checkout", "target_service": "payments",
      "call_count": 2, "duration_ns": 900000000 },
    { "source_service": "payments", "target_service": "payments-db",
      "call_count": 1, "duration_ns": 450000000 }
  ]
}
```

`edges` is sorted by `duration_ns` descending, then source/target service
name ascending. Unknown trace id → `404`.

### `GET /api/v1/traces/{trace_id}/dominant-path` — heaviest root-to-leaf call chain

Ranks every root-to-leaf parent/child chain of one trace by accumulated
**exclusive** weight and returns the heaviest one — the call chain that
dominates the trace's latency. A span's weight is its own `[start, end)`
length minus the union of its direct children's intersections with it
(overlapping or out-of-bounds child intervals are subtracted only once and
only for the part inside the parent interval, so weights never go
negative). Spans whose parent never arrived are treated as roots. Chains
tied on total weight are broken by the lexicographically smallest span-id
sequence. Works on open traces too — computed from a consistent snapshot
of the spans received so far.

```bash
curl -s http://localhost:8080/api/v1/traces/abc-123/dominant-path
```

```json
{
  "trace_id": "abc-123",
  "span_count": 4,
  "path": ["root", "auth", "auth-db"],
  "weights_ns": [300000000, 200000000, 450000000],
  "total_weight_ns": 950000000
}
```

* `path` — span ids of the heaviest chain, root first; `weights_ns` is
  aligned with it position by position and sums to `total_weight_ns`.
* Unknown trace id → `404`.
* A snapshot containing self-referencing spans or parent-reference cycles
  has no root-to-leaf chains → `422` with a `detail` body listing every
  implicated span id, e.g.:

```json
{
  "detail": {
    "message": "trace has no root-to-leaf call chain (parent cycle: x -> y -> z -> x)",
    "span_ids": ["x", "y", "z"],
    "self_references": [],
    "cycles": [["x", "y", "z"]]
  }
}
```

### Other endpoints

| Method & path | Purpose |
| --- | --- |
| `GET /health` | Liveness |
| `GET /api/v1/stats` | Open/pending counts, retained/dropped totals, per-minute budget usage |
| `GET /api/v1/config` | Effective configuration and current minute bucket |
| `POST /api/v1/admin/force-flush` | Close open traces and settle all minutes immediately |

### Reason codes

| Reason | Meaning |
| --- | --- |
| `trace_aggregating` | Silence window / minute settlement not reached yet |
| `error_trace` | Kept by the error priority rule |
| `slow_trace` | Kept by the slow-trace priority rule |
| `priority_budget_exhausted` | Error/slow trace, but the minute budget was full → dropped |
| `stable_hash_selected` | Normal trace; its hash rank was within the remaining budget → kept |
| `stable_hash_not_selected` | Normal trace; lost the hash draw → dropped |
| `normal_budget_exhausted` | Normal trace; no budget remained after priority traces → dropped |

---

## How sampling works

1. **Aggregation.** Spans are merged per `trace_id`; a trace holds the min
   start, max end, span count and an error flag. The trace is considered
   complete when no new span has arrived for `TS_IDLE_WINDOW_SECONDS`
   (default **10 s**).
2. **Classification at close.** Error wins over slow; either makes the trace
   *priority* and it is decided immediately against the budget of the minute
   the trace belongs to (derived from its end time).
3. **Normal traffic waits for the minute to settle.** A minute is settled
   `idle window + one tick` after the minute ends, so all priority traces
   with timestamps in that minute have already been counted. Remaining budget
   slots are then handed out by stable-hash rank across that minute's normal
   traces.
4. **Persistence.** Spans, aggregates, candidates, decisions and minute
   counters are all in SQLite. On startup the process rebuilds its internal
   state from the database: open traces keep waiting, undecided candidates
   are settled, decisions are immutable.

### Configuration (environment variables)

| Variable | Default | Meaning |
| --- | --- | --- |
| `TS_DATABASE_PATH` | `/data/traces.db` | SQLite file |
| `TS_IDLE_WINDOW_SECONDS` | `10` | Silence window that completes a trace |
| `TS_TICK_INTERVAL_SECONDS` | `1` | Background aggregation tick |
| `TS_SLOW_THRESHOLD_NS` | `1000000000` | Trace duration (ns) above which a trace is slow (1 s) |
| `TS_SAMPLE_BUDGET_PER_MINUTE` | `100` | Max retained traces per minute |

---

## Scaling note

The container runs a **single uvicorn worker on purpose**: the per-minute
budget is a serialized in-process decision over one SQLite database. To scale
out, run multiple instances and shard clients by `trace_id` (e.g. hash the
trace id to an instance) so a whole trace always lands on one sampler.

## Development

```bash
pip install -r requirements-dev.txt
pytest -q
```

The test suite covers priority budget saturation, slow/error classification,
stable-hash fill counts, determinism, duplicate/late span handling, batch
queries, timestamp formats, per-service exclusive-time computation,
concurrent-span window detection, trace integrity checks (missing parent,
self reference, cycles, time-boundary violations), cross-service edge
aggregation (call counts, per-parent union durations, clipping, sorting),
dominant-path selection (weights, tie-breaking, missing-parent roots,
cycle/self-reference rejection) and crash recovery from SQLite.
