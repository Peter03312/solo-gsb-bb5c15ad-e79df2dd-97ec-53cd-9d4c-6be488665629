"""FastAPI application: ingest spans, query decisions, inspect stats."""
from __future__ import annotations

import asyncio
import contextlib
import logging

from fastapi import FastAPI, HTTPException, Query

from .config import settings
from .dominantpath import CyclicTraceError
from .engine import SamplingEngine
from .models import (
    BatchDecisionRequest,
    BatchDecisionResponse,
    BatchIngestRequest,
    BatchIngestResponse,
    ConcurrencyResponse,
    DecisionResponse,
    DominantPathResponse,
    IntegrityResponse,
    ServiceEdgesResponse,
    ServiceTimeResponse,
    StatsResponse,
)
from .storage import Storage
from .timeutil import minute_bucket_iso, now_ns

_engine: SamplingEngine | None = None
_storage: Storage | None = None
_bg_task: asyncio.Task | None = None

logger = logging.getLogger("app")


def get_engine() -> SamplingEngine:
    if _engine is None:
        raise RuntimeError("engine not initialized")
    return _engine


async def _aggregator_loop() -> None:
    interval = settings.tick_interval_seconds
    while True:
        try:
            assert _engine is not None
            await asyncio.to_thread(_engine.tick)
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - keep the loop alive
            logger.exception("aggregator tick failed")
        await asyncio.sleep(interval)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    global _engine, _storage, _bg_task
    _storage = Storage(settings.database_path)
    _engine = SamplingEngine(settings, _storage)
    await asyncio.to_thread(_engine.recover)
    _bg_task = asyncio.create_task(_aggregator_loop())
    try:
        yield
    finally:
        assert _bg_task is not None
        _bg_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await _bg_task
        assert _storage is not None
        _storage.close()
        _engine = None
        _storage = None


app = FastAPI(
    title="Tail Sampling Service",
    version="1.0.0",
    description="Distributed-tracing tail sampling with silence-window "
                "aggregation, error/slow priority rules and per-minute "
                "stable-hash budgets.",
    lifespan=lifespan,
)


@app.get("/", tags=["meta"])
def root() -> dict[str, str]:
    return {
        "service": "tail-sampling-service",
        "docs": "/docs",
        "health": "/health",
        "ingest": "POST /api/v1/spans:batch",
        "decisions": "GET /api/v1/decisions/{trace_id}",
        "service_time": "GET /api/v1/traces/{trace_id}/service-time",
        "concurrency": "GET /api/v1/traces/{trace_id}/concurrency?min_active=2",
        "integrity": "GET /api/v1/traces/{trace_id}/integrity",
        "service_edges": "GET /api/v1/traces/{trace_id}/service-edges",
        "dominant_path": "GET /api/v1/traces/{trace_id}/dominant-path",
    }


@app.get("/health", tags=["meta"])
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/api/v1/spans:batch",
    response_model=BatchIngestResponse,
    tags=["ingest"],
    summary="Submit a batch of spans",
)
def ingest_batch(req: BatchIngestRequest) -> BatchIngestResponse:
    result = get_engine().ingest(req.spans)
    return BatchIngestResponse(**result)


@app.get(
    "/api/v1/decisions/{trace_id}",
    response_model=DecisionResponse,
    tags=["decisions"],
    summary="Query the sampling decision for one trace",
)
def get_decision(trace_id: str) -> DecisionResponse:
    view = get_engine().get_decision(trace_id)
    if view is None:
        raise HTTPException(status_code=404, detail="trace not found")
    return DecisionResponse(**view)


@app.post(
    "/api/v1/decisions:batch",
    response_model=BatchDecisionResponse,
    tags=["decisions"],
    summary="Query sampling decisions for many traces",
)
def get_decisions(req: BatchDecisionRequest) -> BatchDecisionResponse:
    views, missing = get_engine().get_decisions(req.trace_ids)
    return BatchDecisionResponse(
        decisions={tid: DecisionResponse(**view) for tid, view in views.items()},
        missing=missing,
    )


@app.get(
    "/api/v1/traces/{trace_id}/service-time",
    response_model=ServiceTimeResponse,
    tags=["traces"],
    summary="Exclusive time spent per service in one trace",
)
def get_service_time(trace_id: str) -> ServiceTimeResponse:
    view = get_engine().get_service_time(trace_id)
    if view is None:
        raise HTTPException(status_code=404, detail="trace not found")
    return ServiceTimeResponse(**view)


@app.get(
    "/api/v1/traces/{trace_id}/concurrency",
    response_model=ConcurrencyResponse,
    tags=["traces"],
    summary="Windows where many spans of one trace ran concurrently",
)
def get_concurrency(
    trace_id: str,
    min_active: int = Query(
        default=2, ge=2,
        description="Minimum number of simultaneously active spans",
    ),
) -> ConcurrencyResponse:
    view = get_engine().get_concurrency(trace_id, min_active)
    if view is None:
        raise HTTPException(status_code=404, detail="trace not found")
    return ConcurrencyResponse(**view)


@app.get(
    "/api/v1/traces/{trace_id}/integrity",
    response_model=IntegrityResponse,
    tags=["traces"],
    summary="Validate parent references and child/parent time containment",
)
def get_integrity(trace_id: str) -> IntegrityResponse:
    view = get_engine().get_integrity(trace_id)
    if view is None:
        raise HTTPException(status_code=404, detail="trace not found")
    return IntegrityResponse(**view)


@app.get(
    "/api/v1/traces/{trace_id}/service-edges",
    response_model=ServiceEdgesResponse,
    tags=["traces"],
    summary="Cross-service direct call edges of one trace",
)
def get_service_edges(trace_id: str) -> ServiceEdgesResponse:
    view = get_engine().get_service_edges(trace_id)
    if view is None:
        raise HTTPException(status_code=404, detail="trace not found")
    return ServiceEdgesResponse(**view)


@app.get(
    "/api/v1/traces/{trace_id}/dominant-path",
    response_model=DominantPathResponse,
    tags=["traces"],
    summary="Heaviest root-to-leaf call chain of one trace",
)
def get_dominant_path(trace_id: str) -> DominantPathResponse:
    try:
        view = get_engine().get_dominant_path(trace_id)
    except CyclicTraceError as exc:
        raise HTTPException(
            status_code=422,
            detail={
                "message": str(exc),
                "span_ids": exc.span_ids,
                "self_references": exc.self_references,
                "cycles": exc.cycles,
            },
        ) from exc
    if view is None:
        raise HTTPException(status_code=404, detail="trace not found")
    return DominantPathResponse(**view)


@app.get("/api/v1/stats", response_model=StatsResponse, tags=["admin"])
def stats() -> StatsResponse:    return StatsResponse(**get_engine().stats())


@app.post("/api/v1/admin/force-flush", tags=["admin"],
          summary="Close all open traces and settle pending buckets")
def force_flush() -> dict[str, object]:
    result = get_engine().force_flush(close_open=True)
    return {"flush": result, "stats": get_engine().stats()}


@app.get("/api/v1/config", tags=["admin"])
def get_config() -> dict[str, object]:
    return {
        "idle_window_seconds": settings.idle_window_seconds,
        "tick_interval_seconds": settings.tick_interval_seconds,
        "slow_threshold_ns": settings.slow_threshold_ns,
        "slow_threshold_seconds": settings.slow_threshold_ns / 1_000_000_000,
        "sample_budget_per_minute": settings.sample_budget_per_minute,
        "current_minute_bucket": minute_bucket_iso(now_ns()),
    }
