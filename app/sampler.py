"""Sampling policy primitives: classification and stable hashing.

The hash is computed from the trace id only, so a given normal trace makes the
same keep/draw choice on every process and every restart (stable hashing).
"""
from __future__ import annotations

import hashlib

# 64-bit hash space.
_MOD = 2**64

# Reason codes returned to clients.
REASON_ERROR = "error_trace"
REASON_SLOW = "slow_trace"
REASON_BUDGET_EXHAUSTED = "priority_budget_exhausted"
REASON_HASH_SELECTED = "stable_hash_selected"
REASON_HASH_NOT_SELECTED = "stable_hash_not_selected"
REASON_NORMAL_NO_BUDGET = "normal_budget_exhausted"
REASON_PENDING = "trace_aggregating"
REASON_UNKNOWN = "unknown_trace"


def stable_hash(trace_id: str) -> int:
    """Return a deterministic 64-bit value for a trace id."""
    digest = hashlib.blake2b(
        trace_id.encode("utf-8"), digest_size=8
    ).digest()
    return int.from_bytes(digest, "big")


def hash_score(trace_id: str) -> float:
    """Uniform value in [0, 1) derived from the trace id."""
    return stable_hash(trace_id) / _MOD


def select_by_hash(trace_ids: list[str], n_keep: int) -> tuple[list[str], list[str]]:
    """Deterministically pick exactly ``n_keep`` trace ids by hash rank.

    Selection is by rank (not per-id threshold) so the configured budget is
    used precisely while remaining stable: with a fixed candidate set the
    same traces always win.
    """
    if n_keep <= 0:
        return [], list(trace_ids)
    ordered = sorted(trace_ids, key=lambda tid: (stable_hash(tid), tid))
    if n_keep >= len(ordered):
        return ordered, []
    kept = ordered[:n_keep]
    dropped = ordered[n_keep:]
    return kept, dropped


def classify(is_error: bool, is_slow: bool) -> str | None:
    """Return the priority reason, or None for ordinary traces."""
    if is_error:
        return REASON_ERROR
    if is_slow:
        return REASON_SLOW
    return None
