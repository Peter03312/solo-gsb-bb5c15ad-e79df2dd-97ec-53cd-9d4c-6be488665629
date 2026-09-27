"""Pytest fixtures: build the app against an isolated temp SQLite database."""
from __future__ import annotations

import os
import tempfile

import pytest

# Configuration must be set before the app modules import ``settings``.
_tmp = tempfile.mkdtemp(prefix="tail-sampler-tests-")
os.environ.setdefault("TS_DATABASE_PATH", os.path.join(_tmp, "test.db"))
os.environ.setdefault("TS_IDLE_WINDOW_SECONDS", "3600")   # only flush on demand
os.environ.setdefault("TS_TICK_INTERVAL_SECONDS", "60")
os.environ.setdefault("TS_SLOW_THRESHOLD_NS", "1000000000")
os.environ.setdefault("TS_SAMPLE_BUDGET_PER_MINUTE", "5")

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402


@pytest.fixture(scope="session")
def client():
    with TestClient(app) as c:
        yield c
