"""
Gate 4 verification — the decision matrix, and Appendix A's `floor` scenario.

Section 2 is the one to lead the video with. Every submission will show a
re-route; almost none will show a system REFUSING to re-route because the only
available person is not good enough, and printing the arithmetic while it does.

    python verify_module4.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from sqlalchemy import select

from alert_router.agent import AlertAgent
from alert_router.channels import ChannelBank
from alert_router.db import build_engine, build_session_factory, init_db
from alert_router.decisions import MATRIX, ROW_IDS
from alert_router.executor import PhaseHooks
from alert_router.models_orm import DispatchAttempt, Evaluation
from alert_router.registry import PresenceBus, Registry, zero_latency
from alert_router.schemas import (
    AlertEvent,
    AttemptState,
    Availability,
    InterruptKind,
    Severity,
)

DB_FILE = Path(__file__).resolve().parent / "verify_module4.db"
RESULTS: list[bool] = []
PRIYA, TOM, ELENA = "stk-001", "stk-002", "stk-003"


def check(label: str, ok: bool, detail: str = "") -> None:
    RESULTS.append(ok)
    print(f"  [{'PASS' if ok else 'FAIL'}] {label:<54} {detail}")


def section(title: str) -> None:
    print(f"\n{title}\n{'-' * len(title)}")


class Gate:
    def __init__(self) -> None:
        self.reached = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, attempt) -> None:
        self.reached.set()
        await self.release.wait()


async def main() -> int:
    for suffix in ("", "-wal", "-shm"):
        Path(str(DB_FILE) + suffix).unlink(missing_ok=True)

    engine = build_engine(f"sqlite+aiosqlite:///{DB_FILE.as_posix()}")
    sessions = build_session_factory(engine)
    await init_db(engine)

    # ── 1. the table itself ─────────────────────────────────────────────────
    section("1. The decision matrix — order is the specification")
    print("  first match wins, top to bottom:")
    for row_id, _, _ in MATRIX:
        print(f"    {row_id}")
    check("eleven rows, R1..R11, contiguous",
          list(ROW_IDS) == [f"R{i}" for i in range(1, 12)])

    # ── 2. the `floor` scenario ─────────────────────────────────────────────
    section("2. Scenario `floor` — the agent refuses to route down (row R4)")

    bus = PresenceBus()
    registry = Registry(sessions, bus=bus, latency=zero_latency)
    bank = ChannelBank(connect_seconds=0.0, send_seconds=0.0)
    gate = Gate()
    abort_seen = asyncio.Event()

    # Tom off the rota: 100 + 8 + 0 = 108, against a floor of 120.
    await registry.set_on_call(TOM, False)

    alert = AlertEvent(
        alert_id="alr-verify-4",
        metric_name="db_replica_lag_seconds",
        value=94.0,
        threshold=30.0,
        severity=Severity.CRITICAL,
        domain="infrastructure",
    )

    agent = AlertAgent(registry, sessions, bank, hooks=PhaseHooks(on_in_flight=gate))
    original_handler = agent.on_interrupt

    async def watched(state, event, attempt):
        decision = await original_handler(state, event, attempt)
        if event.kind is InterruptKind.PRESENCE_CHANGED:
            abort_seen.set()
        return decision

    agent.on_interrupt = watched
    agent.listener = None

    task = asyncio.create_task(agent.handle(alert))
    await asyncio.wait_for(gate.reached.wait(), 5.0)
    print(f"  in flight to {agent.executor.current_attempt.stakeholder_id} on "
          f"{agent.executor.current_attempt.channel.value}")

    await registry.set_status(PRIYA, Availability.OFFLINE, reason="laptop closed")
    await asyncio.wait_for(abort_seen.wait(), 5.0)
    gate.release.set()
    state = await asyncio.wait_for(task, 15.0)

    fired = [d.matrix_row for d in agent.decisions]
    print(f"  matrix rows fired: {fired}")
    check("R4 fired", "R4" in fired, "refused to route down")

    r4 = next((d for d in agent.decisions if d.matrix_row == "R4"), None)
    if r4 is not None:
        print(f"  rationale: {r4.rationale[:100]}")

    section("3. R4's three obligations")
    check("1 - incumbent kept, moved to a persistent channel",
          PRIYA in state.notified and state.attempted[PRIYA].channel.is_persistent,
          f"{state.attempted[PRIYA].channel.value}")
    check("2 - the most QUALIFIED member was paged",
          ELENA in state.notified, "Elena 140, offline, on a persistent channel")
    check("3 - the under-qualified junior is on record with the arithmetic",
          TOM in state.suppressed)
    if TOM in state.suppressed:
        print(f"       {state.suppressed[TOM]}")
        check("   the reason carries 108, 120 and 139",
              all(t in state.suppressed[TOM] for t in ("108", "120", "139")))
    check("Tom was NOT notified", TOM not in state.notified,
          "routing down is what invariant I4 forbids")

    section("4. Attempt rows — two people, two keys, no duplicates")
    async with sessions() as session:
        rows = (
            await session.scalars(
                select(DispatchAttempt)
                .where(DispatchAttempt.alert_id == alert.alert_id)
                .order_by(DispatchAttempt.id)
            )
        ).all()
    print(f"  {'who':<10}{'role':<12}{'channel':<9}{'state':<12}{'key'}")
    print(f"  {'-' * 74}")
    for row in rows:
        print(f"  {row.stakeholder_id:<10}{row.role:<12}{row.channel:<9}"
              f"{row.state:<12}{row.idempotency_key}")

    committed = [r for r in rows if r.state == "committed"]
    keys = [r.idempotency_key for r in rows]
    check("every idempotency key is unique", len(keys) == len(set(keys)))
    check("two people committed", len(committed) == 2, str(sorted(state.notified)))
    check("Priya has exactly ONE row despite the channel change",
          len([r for r in rows if r.stakeholder_id == PRIYA]) == 1,
          "failover is an UPDATE, not a second dispatch")

    section("5. The query budget, still frozen")
    async with sessions() as session:
        pulls = (
            await session.scalars(
                select(Evaluation.stakeholder_id).where(
                    Evaluation.alert_id == alert.alert_id
                )
            )
        ).all()
    check("7 evaluations, one per candidate", len(pulls) == 7, str(len(pulls)))
    check("nobody queried twice", len(pulls) == len(set(pulls)))

    section("6. The audit trail, with matrix rows in it")
    for line in state.audit_lines():
        print(f"    {line[:110]}")
    kinds = [e.kind for e in state.audit]
    check("a DECISION row is recorded", "DECISION" in kinds)
    check("a SUPPRESSED row is recorded", "SUPPRESSED" in kinds)
    check("the trail names a matrix row",
          any("[R" in e.summary for e in state.audit))
    check("seq is contiguous",
          [e.seq for e in state.audit] == list(range(len(state.audit))))

    bus.close()
    await engine.dispose()
    for suffix in ("", "-wal", "-shm"):
        Path(str(DB_FILE) + suffix).unlink(missing_ok=True)

    print(f"\n{'=' * 74}")
    print(f"  {sum(RESULTS)}/{len(RESULTS)} checks passed")
    print(f"{'=' * 74}")
    return 0 if all(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
