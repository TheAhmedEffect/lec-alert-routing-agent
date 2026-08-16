"""
The four demo scenarios — defined ONCE and shared by the CLI, the tests and the
verification scripts.

WHY THIS FILE EXISTS
--------------------
Modules 3, 4 and 5 each grew their own copy of the same scaffolding: a phase
gate, a watched interrupt handler, and an asyncio.Event to close the race
between "the event was published" and "the listener actually dequeued it".

Three copies of race-sensitive setup is three places for the race to come back —
and the failure mode is a demo that shows the wrong decision row on camera, once
in twenty runs, with no way to explain it. So the plumbing lives here, once, and
everything else calls it.

EACH SCENARIO GETS ITS OWN DATABASE
-----------------------------------
`--scenario all` runs four alerts through one process. Sharing a database means
alert ids, idempotency keys and evaluation rows collide, and scenario two fails
on constraints that are working perfectly. A fresh engine per scenario removes
the whole class of problem, and it is also honest: each scenario is a separate
incident, not four incidents on one system.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Awaitable, Callable

from .agent import AlertAgent
from .channels import ChannelBank
from .db import build_engine, build_session_factory, init_db
from .executor import PhaseHooks
from .registry import PresenceBus, Registry, zero_latency
from .schemas import (
    AlertEvent,
    Availability,
    Channel,
    DispatchPlan,
    RoutingDecision,
    Severity,
)
from .state import DispatchState

PRIYA, TOM, ELENA, DANIEL = "stk-001", "stk-002", "stk-003", "stk-007"

SCENARIO_NAMES: tuple[str, ...] = ("reroute", "floor", "escalate", "failover")


# ─────────────────────────────────────────────────────────────────────────────
# Shared plumbing
# ─────────────────────────────────────────────────────────────────────────────


class PhaseGate:
    """Parks the dispatch at a known phase until released.

    This is what makes the scenarios deterministic. "Inject the interrupt at
    t+0.8s" races the scheduler and eventually shows the wrong matrix row; a gate
    makes pre-commit versus post-commit something the scenario CHOOSES.
    """

    def __init__(self) -> None:
        self.reached = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, attempt) -> None:
        self.reached.set()
        await self.release.wait()

    async def wait(self, timeout: float = 10.0) -> None:
        await asyncio.wait_for(self.reached.wait(), timeout)

    def open(self) -> None:
        self.release.set()


class Harness:
    """Wraps the agent's interrupt handler so a scenario can wait for the
    decision to have been MADE before releasing the gate.

    Without this, `set_status()` only publishes — releasing the gate immediately
    afterwards lets the dispatch finish before the listener has even dequeued
    the event, and the scenario silently proves nothing.
    """

    def __init__(self, agent: AlertAgent) -> None:
        self.agent = agent
        self.handled = asyncio.Event()
        self._inner = agent.on_interrupt
        agent.on_interrupt = self._watch

    async def _watch(self, state, event, attempt):
        decision = await self._inner(state, event, attempt)
        self.handled.set()
        return decision

    async def wait(self, timeout: float = 10.0) -> None:
        await asyncio.wait_for(self.handled.wait(), timeout)


@dataclass
class ScenarioResult:
    name: str
    headline: str
    expected_row: str
    alert: AlertEvent
    state: DispatchState
    decisions: list[RoutingDecision] = field(default_factory=list)
    delivered: list[tuple[str, str]] = field(default_factory=list)
    query_count: int = 0

    @property
    def plan(self) -> DispatchPlan:
        return self.state.plan

    @property
    def rows_fired(self) -> list[str]:
        return [d.matrix_row for d in self.decisions]

    @property
    def matched_expectation(self) -> bool:
        return self.expected_row in self.rows_fired

    @property
    def notified(self) -> list[str]:
        return sorted(self.state.notified)


Injector = Callable[[Registry, AlertAgent, PhaseGate, Harness, ChannelBank], Awaitable[None]]


# ─────────────────────────────────────────────────────────────────────────────
# The alerts
# ─────────────────────────────────────────────────────────────────────────────


def _replica_lag(alert_id: str) -> AlertEvent:
    return AlertEvent(
        alert_id=alert_id,
        metric_name="db_replica_lag_seconds",
        value=94.0,
        threshold=30.0,
        direction="above",
        severity=Severity.CRITICAL,
        domain="infrastructure",
    )


def _stock_depletion(alert_id: str) -> AlertEvent:
    """A DEPLETION breach — it crosses the threshold by FALLING.

    A router that hardcodes `value > threshold` drops a stock-out silently,
    which is the worst possible failure for an alerting system: no signal, no
    error, no record. `direction` is a first-class field for exactly this.
    """
    return AlertEvent(
        alert_id=alert_id,
        metric_name="warehouse_stock_units",
        value=12.0,
        threshold=50.0,
        direction="below",
        severity=Severity.HIGH,
        domain="logistics",
    )


# ─────────────────────────────────────────────────────────────────────────────
# The injections — one per scenario
# ─────────────────────────────────────────────────────────────────────────────


async def _inject_presence_drop(
    registry: Registry, agent: AlertAgent, gate: PhaseGate, harness: Harness, bank
) -> None:
    """Shared by `reroute` and `floor`: the incumbent drops offline mid-Slack."""
    await gate.wait()
    await registry.set_status(PRIYA, Availability.OFFLINE, reason="laptop closed")
    await harness.wait()
    gate.open()


async def _inject_elena_comes_online(
    registry: Registry, agent: AlertAgent, gate: PhaseGate, harness: Harness, bank
) -> None:
    """`escalate`: a MORE QUALIFIED person becomes reachable mid-dispatch.

    Note what does NOT happen: no pull. The listener patches Elena's cached
    snapshot from the event payload, re-scores against facts already bought, and
    derives BETTER_MATCH. Row R9 then pages her in parallel while Priya's
    original send completes.
    """
    await gate.wait()
    await registry.set_status(ELENA, Availability.ONLINE, reason="picked up the page")
    await harness.wait()
    gate.open()


async def _inject_slack_outage(
    registry: Registry, agent: AlertAgent, gate: PhaseGate, harness: Harness, bank
) -> None:
    """`failover`: the transport dies under a healthy person.

    A person is not their transport. Row R6 changes the pipe and keeps the
    recipient — same attempt row, same idempotency key — because changing person
    in response to a broken wire is an over-reaction.
    """
    await gate.wait()
    bank.fail(Channel.SLACK, on_connect=True)
    await registry.set_channel_health(
        DANIEL, Channel.SLACK, healthy=False, last_error="slack adapter refused"
    )
    await harness.wait()
    gate.open()


# ─────────────────────────────────────────────────────────────────────────────
# The scenario table
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ScenarioSpec:
    name: str
    headline: str
    expected_row: str
    build_alert: Callable[[str], AlertEvent]
    inject: Injector
    #: Applied before the alert fires. `floor` uses it to take Tom off the rota.
    prepare: Callable[[Registry], Awaitable[None]] | None = None


async def _take_tom_off_the_rota(registry: Registry) -> None:
    """Drops Tom from 123 to 108, under the CRITICAL floor of 120.

    One boolean is the difference between "re-route to Tom" and "refuse to route
    down at all" — which is the entire point of invariant I4.
    """
    await registry.set_on_call(TOM, False)


SPECS: dict[str, ScenarioSpec] = {
    "reroute": ScenarioSpec(
        name="reroute",
        headline="Incumbent drops offline mid-Slack; a qualified replacement exists",
        expected_row="R3",
        build_alert=_replica_lag,
        inject=_inject_presence_drop,
    ),
    "floor": ScenarioSpec(
        name="floor",
        headline="Same failure, but every reachable alternative is under-qualified",
        expected_row="R4",
        build_alert=_replica_lag,
        inject=_inject_presence_drop,
        prepare=_take_tom_off_the_rota,
    ),
    "escalate": ScenarioSpec(
        name="escalate",
        headline="A more senior stakeholder becomes available mid-dispatch",
        expected_row="R9",
        build_alert=_replica_lag,
        inject=_inject_elena_comes_online,
    ),
    "failover": ScenarioSpec(
        name="failover",
        headline="The transport dies, not the person (and a below-threshold breach)",
        expected_row="R6",
        build_alert=_stock_depletion,
        inject=_inject_slack_outage,
    ),
}


# ─────────────────────────────────────────────────────────────────────────────
# The runner
# ─────────────────────────────────────────────────────────────────────────────


async def run_scenario(
    name: str,
    *,
    db_path: Path | None = None,
    send_seconds: float = 0.0,
    timeout: float = 30.0,
) -> ScenarioResult:
    """Run one scenario against its OWN database, and return what happened.

    A fresh engine per scenario is what lets `--scenario all` work: four alerts
    in one process would otherwise collide on alert ids and idempotency keys,
    and fail on constraints that are behaving perfectly.
    """
    if name not in SPECS:
        raise ValueError(f"unknown scenario {name!r}; choose from {SCENARIO_NAMES}")
    spec = SPECS[name]

    db_path = db_path or Path.cwd() / f"scenario_{name}.db"
    for suffix in ("", "-wal", "-shm"):
        Path(str(db_path) + suffix).unlink(missing_ok=True)

    engine = build_engine(f"sqlite+aiosqlite:///{db_path.as_posix()}")
    sessions = build_session_factory(engine)
    await init_db(engine)

    bus = PresenceBus()
    registry = Registry(sessions, bus=bus, latency=zero_latency)
    bank = ChannelBank(connect_seconds=0.0, send_seconds=send_seconds)
    gate = PhaseGate()

    if spec.prepare is not None:
        await spec.prepare(registry)

    agent = AlertAgent(registry, sessions, bank, hooks=PhaseHooks(on_in_flight=gate))
    harness = Harness(agent)
    alert = spec.build_alert(f"alr-{name}")

    try:
        dispatch = asyncio.create_task(agent.handle(alert))
        injection = asyncio.create_task(
            spec.inject(registry, agent, gate, harness, bank)
        )
        await asyncio.wait_for(asyncio.gather(dispatch, injection), timeout)
        state = dispatch.result()
        query_count = await registry.evaluation_count(alert.alert_id)
    finally:
        gate.open()          # never leave a parked dispatch behind
        bus.close()
        await engine.dispose()

    return ScenarioResult(
        name=spec.name,
        headline=spec.headline,
        expected_row=spec.expected_row,
        alert=alert,
        state=state,
        decisions=list(agent.decisions),
        delivered=[(channel.value, recipient) for channel, recipient, _ in bank.delivered],
        query_count=query_count,
    )


async def run_all(
    *, db_dir: Path | None = None, send_seconds: float = 0.0
) -> list[ScenarioResult]:
    """Every scenario, each on its own clean database."""
    results = []
    for name in SCENARIO_NAMES:
        path = (db_dir / f"scenario_{name}.db") if db_dir else None
        results.append(
            await run_scenario(name, db_path=path, send_seconds=send_seconds)
        )
    return results


def cleanup(db_dir: Path | None = None) -> None:
    """Remove scenario databases, including the WAL sidecars."""
    base = db_dir or Path.cwd()
    for name in SCENARIO_NAMES:
        for suffix in ("", "-wal", "-shm"):
            Path(str(base / f"scenario_{name}.db") + suffix).unlink(missing_ok=True)
