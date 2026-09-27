#!/usr/bin/env python3
"""Zero-dependency demo client (stdlib only).

Submits a mix of normal, error and slow traces to a running service, forces a
sampling pass, and prints the decisions.

Usage:
    python scripts/demo_client.py [base_url] [budget]
    python scripts/demo_client.py http://localhost:8080
"""
from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request
import uuid


def call(base: str, method: str, path: str, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        base.rstrip("/") + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def main() -> int:
    base = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080"

    spans = []
    normal = [f"demo-normal-{uuid.uuid4().hex[:10]}-{i}" for i in range(12)]
    errors = [f"demo-error-{uuid.uuid4().hex[:10]}-{i}" for i in range(3)]
    slows = [f"demo-slow-{uuid.uuid4().hex[:10]}-{i}" for i in range(2)]

    now_s = time.time()
    for i, tid in enumerate(normal):
        spans.append({
            "trace_id": tid, "span_id": "root", "service": "checkout",
            "name": "GET /cart",
            "start_time": now_s + i * 0.001,
            "end_time": now_s + i * 0.001 + 0.05,       # 50 ms — healthy
            "status": "ok",
            "attributes": {"http.status_code": 200},
        })
    for i, tid in enumerate(errors):
        spans.append({
            "trace_id": tid, "span_id": "root", "service": "payments",
            "name": "POST /charge",
            "start_time": now_s + i * 0.001,
            "end_time": now_s + i * 0.001 + 0.2,
            "status": "error",
            "attributes": {"http.status_code": 500, "exception.type": "RuntimeError"},
        })
    for i, tid in enumerate(slows):
        spans.append({
            "trace_id": tid, "span_id": "root", "service": "search",
            "name": "GET /query",
            "start_time": now_s + i * 0.001,
            "end_time": now_s + i * 0.001 + 2.5,        # 2.5 s — slow
            "status": "ok",
            "attributes": {"http.status_code": 200},
        })

    print(f"POST {len(spans)} spans ->", base)
    result = call(base, "POST", "/api/v1/spans:batch", {"spans": spans})
    print(f"  accepted={result['accepted']} "
          f"duplicates={result['duplicates']} rejected={result['rejected']}")

    # In production the silence window closes traces automatically; the demo
    # does not want to wait, so force the pass.
    call(base, "POST", "/api/v1/admin/force-flush")

    all_ids = normal + errors + slows
    decisions = call(base, "POST", "/api/v1/decisions:batch",
                     {"trace_ids": all_ids})["decisions"]

    for group, label in ((errors, "ERROR "), (slows, "SLOW  "),
                         (normal, "NORMAL")):
        print(f"\n[{label}]")
        for tid in group:
            d = decisions[tid]
            print(f"  {d['decision']:4} {d['reason']:28} {tid}")

    stats = call(base, "GET", "/api/v1/stats")
    print("\n[STATS]")
    print(json.dumps(
        {k: v for k, v in stats.items() if k != "budget"}, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except urllib.error.URLError as exc:
        print(f"cannot reach service: {exc}", file=sys.stderr)
        raise SystemExit(1)
