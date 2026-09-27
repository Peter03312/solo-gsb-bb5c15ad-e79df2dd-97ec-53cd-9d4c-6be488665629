"""Pydantic request/response schemas."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .timeutil import to_ns

# Accept the common OpenTelemetry spellings plus our canonical lower case.
SpanStatus = Literal["ok", "error", "unset", "OK", "ERROR", "UNSET",
                     "Ok", "Error", "Unset"]


class SpanIn(BaseModel):
    model_config = ConfigDict(extra="ignore")

    trace_id: str = Field(..., min_length=1, description="W3C-style trace id")
    span_id: str = Field(..., min_length=1)
    parent_span_id: str | None = None
    name: str | None = None
    service: str | None = None
    # ISO-8601 string or epoch seconds/ms/us/ns number.
    start_time: float | int | str | datetime
    end_time: float | int | str | datetime
    status: SpanStatus = "unset"
    attributes: dict[str, Any] = Field(default_factory=dict)

    @field_validator("trace_id", "span_id", "parent_span_id", "name", "service")
    @classmethod
    def _strip(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = v.strip()
        return v or None

    @field_validator("status", mode="before")
    @classmethod
    def _status_lower(cls, v: Any) -> Any:
        return v.lower() if isinstance(v, str) else v

    @field_validator("start_time", "end_time")
    @classmethod
    def _valid_time(cls, v: Any) -> Any:
        try:
            to_ns(v)
        except (ValueError, OverflowError, TypeError) as exc:
            raise ValueError(f"invalid timestamp: {v!r}") from exc
        return v


class BatchIngestRequest(BaseModel):
    spans: list[SpanIn] = Field(..., min_length=1)


class BatchIngestResponse(BaseModel):
    accepted: int
    rejected: int
    duplicates: int
    # Trace ids seen in this batch with their current state. Decisions for
    # closed traces appear immediately; open traces show state="pending".
    traces: dict[str, dict[str, Any]]


class DecisionResponse(BaseModel):
    trace_id: str
    state: Literal["unknown", "open", "pending", "priority_kept",
                   "normal_kept", "dropped"]
    decision: Literal["keep", "drop", "pending"]
    reason: str | None = None
    is_error: bool | None = None
    is_slow: bool | None = None
    duration_ns: int | None = None
    span_count: int | None = None
    minute_bucket: int | None = None
    decided_at_ns: int | None = None


class BatchDecisionRequest(BaseModel):
    trace_ids: list[str] = Field(..., min_length=1)


class BatchDecisionResponse(BaseModel):
    decisions: dict[str, DecisionResponse]
    missing: list[str]


class ServiceTimeEntry(BaseModel):
    # null groups spans without a service attribute.
    service: str | None = None
    span_count: int
    exclusive_ns: int


class ServiceTimeResponse(BaseModel):
    trace_id: str
    span_count: int
    services: list[ServiceTimeEntry]


class ConcurrencyWindow(BaseModel):
    start_ns: int
    end_ns: int
    duration_ns: int
    peak_active: int


class ConcurrencyResponse(BaseModel):
    trace_id: str
    min_active: int
    span_count: int
    window_count: int
    # Total time (ns) with at least `min_active` spans active at once.
    total_concurrent_ns: int
    windows: list[ConcurrencyWindow]


class IntegrityAnomaly(BaseModel):
    # missing_parent  - parent_span_id names a span the trace never received
    # self_reference  - a span lists itself as its parent
    # cycle           - >=2 spans form a parent-reference ring
    # time_out_of_bounds - the child interval is not within the parent interval
    type: Literal["missing_parent", "self_reference", "cycle",
                  "time_out_of_bounds"]
    # Spans implicated, sorted deterministically: the lone offender for
    # missing-parent/self-reference, every ring member (in ring order) for a
    # cycle, and [child, parent] for a time-boundary violation.
    span_ids: list[str]
    # The referenced parent (null for cycles and for root-level checks).
    parent_span_id: str | None = None
    message: str
    # Time-boundary details only; null for reference anomalies.
    child_start_ns: int | None = None
    child_end_ns: int | None = None
    parent_start_ns: int | None = None
    parent_end_ns: int | None = None
    start_overflow_ns: int | None = None
    end_overflow_ns: int | None = None


class IntegrityResponse(BaseModel):
    trace_id: str
    span_count: int
    anomaly_count: int
    # True when the current snapshot has neither reference nor time-boundary
    # problems (anomaly_count == 0).
    valid: bool
    anomalies: list[IntegrityAnomaly]


class ServiceEdge(BaseModel):
    source_service: str
    target_service: str
    call_count: int
    duration_ns: int


class ServiceEdgesResponse(BaseModel):
    trace_id: str
    span_count: int
    edge_count: int
    edges: list[ServiceEdge]


class DominantPathResponse(BaseModel):
    trace_id: str
    span_count: int
    # Span ids of the heaviest root-to-leaf chain, root first.
    path: list[str]
    # Exclusive weight of each node on `path`, aligned position by position;
    # the entries sum to total_weight_ns.
    weights_ns: list[int]
    total_weight_ns: int


class StatsResponse(BaseModel):
    open_traces: int
    pending_normal: int
    decided_traces: int
    retained: int
    dropped: int
    budget: dict[str, dict[str, int]]
