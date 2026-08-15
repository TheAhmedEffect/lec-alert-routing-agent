"""
Shared fixtures.

TWO THINGS HERE ARE LOAD-BEARING
--------------------------------
1. The test database is a FILE under pytest's tmp_path, not `:memory:`.
   With a connection pool, an in-memory SQLite database is per-connection:
   init_db() creates the schema on connection 1, the test queries on
   connection 2, and the test fails with "no such table". The alternative fix
   is StaticPool, which forces every checkout onto one connection — but that
   would also make test_pragma_foreign_keys_on_for_every_connection vacuous,
   since there would only ever be one connection. A tmp_path file gives real
   pooling AND a real second connection to assert against.

2. Engines are built through db.build_engine(), never create_async_engine()
   directly. build_engine attaches the PRAGMA connect listener. A test engine
   without it would silently lose foreign-key enforcement, and the suite would
   then be proving something weaker than it claims.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from alert_router import config
from alert_router.db import build_engine, build_session_factory, init_db
from alert_router.registry import PresenceBus, Registry, zero_latency
from alert_router.schemas import AlertEvent, Severity


class StepClock:
    """Deterministic monotonic clock.

    Advances one second per call, so `observed_at` values are ordered and
    reproducible. freezegun would pin time to a constant instead, which makes
    two observations indistinguishable — the opposite of what a system built
    around point-in-time snapshots wants from its tests.
    """

    def __init__(self, start: float = 1_000_000.0, step: float = 1.0) -> None:
        self.now = start
        self.step = step

    def __call__(self) -> float:
        value = self.now
        self.now += self.step
        return value


@pytest.fixture
def db_url(tmp_path: Path) -> str:
    # as_posix() because this project is built on Windows, where str(Path)
    # produces backslashes that do not belong in a URL.
    return f"sqlite+aiosqlite:///{(tmp_path / 'test.db').as_posix()}"


@pytest.fixture
async def engine(db_url: str):
    """A fresh, seeded database per test. No state leaks between tests."""
    engine = build_engine(db_url)
    await init_db(engine, seed_path=config.SEED_PATH)
    yield engine
    await engine.dispose()


@pytest.fixture
def session_factory(engine):
    return build_session_factory(engine)


@pytest.fixture
def bus():
    """A PresenceBus that is always closed, even if the test fails.

    Without the close() in teardown, any subscription left iterating would
    block forever and the whole suite would hang with no output.
    """
    presence_bus = PresenceBus()
    yield presence_bus
    presence_bus.close()


@pytest.fixture
def clock() -> StepClock:
    return StepClock()


@pytest.fixture
def registry(session_factory, bus, clock) -> Registry:
    """Zero latency injected.

    The real registry sleeps 120-300ms per pull, which is essential to the
    demo — it is what creates the mid-flight window — and fatal to a test
    suite. A suite slow enough to be annoying is a suite that stops being run.
    """
    return Registry(session_factory, bus=bus, clock=clock, latency=zero_latency)


@pytest.fixture
def critical_alert() -> AlertEvent:
    """Appendix A's `reroute` / `floor` / `escalate` alert."""
    return AlertEvent(
        alert_id="alr-test-critical",
        metric_name="db_replica_lag_seconds",
        value=94.0,
        threshold=30.0,
        direction="above",
        severity=Severity.CRITICAL,
        domain="infrastructure",
        triggered_at=1_000_000.0,
    )


@pytest.fixture
def stock_alert() -> AlertEvent:
    """Appendix A's `failover` alert — note direction='below'.

    A depletion breach. A router hardcoding `value > threshold` drops this
    silently, which is why direction is a first-class field.
    """
    return AlertEvent(
        alert_id="alr-test-stock",
        metric_name="warehouse_stock_units",
        value=12.0,
        threshold=50.0,
        direction="below",
        severity=Severity.HIGH,
        domain="logistics",
        triggered_at=1_000_000.0,
    )
