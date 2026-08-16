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

import asyncio
from pathlib import Path

import pytest

from alert_router import config
from alert_router.agent import AlertAgent
from alert_router.channels import ChannelBank
from alert_router.executor import DispatchExecutor
from alert_router.db import build_engine, build_session_factory, init_db
from alert_router.ranking import build_ladder
from alert_router.registry import PresenceBus, Registry, zero_latency
from alert_router.schemas import AlertEvent, DispatchPlan, Severity
from alert_router.state import DispatchState


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
async def snapshots(registry, critical_alert):
    """The seven infrastructure candidates from ONE pull.

    Spending the query budget in a fixture is deliberate: every ranking test
    then works from the same single observation, exactly as the real system
    does, and no test can accidentally pull twice.
    """
    return await registry.query_by_domain(critical_alert)


@pytest.fixture
def plan(snapshots, critical_alert, clock) -> DispatchPlan:
    return build_ladder(snapshots, critical_alert, clock=clock)


@pytest.fixture
def state(critical_alert, snapshots, session_factory, clock) -> DispatchState:
    return DispatchState.start(
        critical_alert, snapshots, session_factory=session_factory, clock=clock
    )


@pytest.fixture
def offline_state(critical_alert, snapshots, clock) -> DispatchState:
    """A state with NO session factory — pure in-memory.

    Used for the audit-sequence concurrency test, where the unit under test is
    the lock rather than the database, and 20 concurrent SQLite writes would add
    contention noise without adding evidence.
    """
    return DispatchState.start(critical_alert, snapshots, clock=clock)


class PhaseGate:
    """Park the executor at a known phase until the test releases it.

    THIS IS WHY MODULE 3'S SUITE IS NOT FLAKY. Landing an interrupt "at t+0.8s"
    races the scheduler: it passes on one machine and fails on another, and the
    failure looks like a routing bug. A gate makes pre-commit and post-commit
    something the test CHOOSES rather than something it hopes for.
    """

    def __init__(self) -> None:
        self.reached = asyncio.Event()
        self.release = asyncio.Event()
        self.hits = 0

    async def __call__(self, attempt) -> None:
        self.hits += 1
        self.reached.set()
        await self.release.wait()

    async def wait(self, timeout: float = 3.0) -> None:
        """Block until the executor has parked here."""
        await asyncio.wait_for(self.reached.wait(), timeout)

    def open(self) -> None:
        self.release.set()


@pytest.fixture
def gate() -> PhaseGate:
    return PhaseGate()


@pytest.fixture
def bank() -> ChannelBank:
    """Adapters with near-zero latency. The demo uses realistic delays; the
    suite must not, or nobody runs it."""
    return ChannelBank(connect_seconds=0.0, send_seconds=0.0)


@pytest.fixture
def executor(session_factory, bank, clock) -> DispatchExecutor:
    return DispatchExecutor(session_factory, bank, clock=clock)


@pytest.fixture
def agent(registry, session_factory, bank, clock) -> AlertAgent:
    return AlertAgent(registry, session_factory, bank, clock=clock)


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
