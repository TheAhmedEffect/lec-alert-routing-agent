"""
Gate 3 verification — the abort window, live.

Runs Appendix A's `reroute` scenario end to end and prints what happened: the
ladder, the interrupt landing mid-send, the abort inside the window, the
re-route, and — the number that matters — the query counter refusing to move.

    python verify_module3.py

Sections 3 and 5 are what you narrate at 0:50-1:30 in the walkthrough.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from sqlalchemy import func, select

from alert_router.agent import AlertAgent
from alert_router.channels import ChannelBank
from alert_router.db import build_engine, build_session_factory, init_db
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

DB_FILE = Path(__file__).resolve().parent / "verify_module3.db"
RESULTS: list[bool] = []
PRIYA, TOM, ELENA = "stk-001", "stk-002", "stk-003"


def check(label: str, ok: bool, detail: str = "") -> None:
    RESULTS.append(ok)
    print(f"  [{'PASS' if ok else 'FAIL'}] {label:<54} {detail}")


def section(title: str) -> None:
    print(f"\n{title}\n{'-' * len(title)}")


class Gate:
    """Parks the dispatch mid-send so the interrupt lands deterministically."""

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

    bus = PresenceBus()
    registry = Registry(sessions, bus=bus, latency=zero_latency)
    bank = ChannelBank(connect_seconds=0.0, send_seconds=0.0)
    gate = Gate()

    alert = AlertEvent(
        alert_id="alr-verify-3",
        metric_name="db_replica_lag_seconds",
        value=94.0,
        threshold=30.0,
        severity=Severity.CRITICAL,
        domain="infrastructure",
    )

    seen: list[str] = []
    # Set once the abort has actually been requested. set_status() only
    # PUBLISHES; releasing the gate before the listener has dequeued the event
    # would let the dispatch finish first and the scenario would race.
    abort_requested = asyncio.Event()

    async def abort_on_presence_drop(state, event, attempt):
        seen.append(f"{event.kind.value}:{event.stakeholder_id}")
        if event.kind is InterruptKind.PRESENCE_CHANGED and event.went_offline:
            aborted = agent.abort_current(
                f"{event.stakeholder_id} went offline mid-send"
            )
            print(f"         interrupt handled -> abort_current() returned {aborted}")
            abort_requested.set()

    agent = AlertAgent(
        registry,
        sessions,
        bank,
        hooks=PhaseHooks(on_in_flight=gate),
        on_interrupt=abort_on_presence_drop,
    )

    # ── run the scenario ────────────────────────────────────────────────────
    section("1. Dispatch begins")
    task = asyncio.create_task(agent.handle(alert))
    await asyncio.wait_for(gate.reached.wait(), 5.0)
    attempt = agent.executor.current_attempt
    print(f"  in flight to {attempt.stakeholder_id} on {attempt.channel.value}, "
          f"state={attempt.state.value}")
    check("attempt is pre-commit", attempt.state.is_pre_commit)
    check("nothing delivered yet", bank.delivered == [])

    section("2. Mid-flight, the recipient drops offline")
    await registry.set_status(PRIYA, Availability.OFFLINE, reason="laptop closed")
    await asyncio.wait_for(abort_requested.wait(), 5.0)
    gate.release.set()
    state = await asyncio.wait_for(task, 10.0)
    check("the presence event was seen by the listener", any("presence" in s for s in seen), str(seen))

    # ── what happened ───────────────────────────────────────────────────────
    section("3. The ladder, and where the dispatch went")
    print(f"  {'rank':<5}{'name':<17}{'qual':>5}  {'reach':<7}{'outcome'}")
    print(f"  {'-' * 60}")
    for candidate in state.plan.ladder:
        person = candidate.snapshot.stakeholder
        record = state.attempted.get(person.id)
        outcome = record.state.value if record else ""
        if person.id in state.notified:
            outcome += "  <- NOTIFIED"
        print(f"  {candidate.rank:<5}{person.name:<17}"
              f"{candidate.score.qualification:>5.0f}  "
              f"{candidate.score.reachability:<7}{outcome}")

    check("Priya's attempt is ABORTED", state.attempted[PRIYA].state is AttemptState.ABORTED)
    check("Tom was notified", TOM in state.notified)
    check("Priya was NOT notified", PRIYA not in state.notified)
    check("exactly one person notified", len(state.notified) == 1, str(sorted(state.notified)))

    delivered = [(c.value, r) for c, r, _ in bank.delivered]
    check("only Tom actually received a message", delivered == [("slack", TOM)], str(delivered))

    section("4. The attempt rows — invariants I2 and the commit-time CHECK")
    async with sessions() as session:
        rows = (
            await session.scalars(
                select(DispatchAttempt)
                .where(DispatchAttempt.alert_id == alert.alert_id)
                .order_by(DispatchAttempt.id)
            )
        ).all()
    print(f"  {'who':<10}{'role':<11}{'state':<12}{'committed_at':<14}{'key'}")
    print(f"  {'-' * 72}")
    for row in rows:
        stamp = f"{row.committed_at:.0f}" if row.committed_at else "-"
        print(f"  {row.stakeholder_id:<10}{row.role:<11}{row.state:<12}"
              f"{stamp:<14}{row.idempotency_key}")

    check("two attempts: aborted then committed",
          [(r.stakeholder_id, r.state) for r in rows]
          == [(PRIYA, "aborted"), (TOM, "committed")])
    check("roles are primary then reroute", [r.role for r in rows] == ["primary", "reroute"])
    check("aborted row has NO committed_at", rows[0].committed_at is None)
    check("committed row HAS committed_at", rows[1].committed_at is not None)
    check("idempotency keys exclude the channel",
          all(":" in r.idempotency_key and r.channel not in r.idempotency_key for r in rows))

    section("5. The number that matters — the query budget")
    async with sessions() as session:
        pulls = await session.scalar(
            select(func.count()).select_from(Evaluation).where(
                Evaluation.alert_id == alert.alert_id
            )
        )
        sources = set(
            (await session.scalars(
                select(Evaluation.source).where(Evaluation.alert_id == alert.alert_id)
            )).all()
        )
    print(f"  evaluations rows for this alert : {pulls}")
    print(f"  distinct `source` values        : {sorted(sources)}")
    print("  the agent learned Priya went offline, changed its mind, and routed to")
    print("  a different person — without asking the registry a single new question.")
    check("still exactly 7 rows — one per candidate", pulls == 7, str(pulls))
    check("every row says 'pull'", sources == {"pull"})
    check("nobody was queried twice", len(state.evaluated) == 7)

    section("6. The audit trail")
    for line in state.audit_lines():
        print(f"    {line[:104]}")
    kinds = [e.kind for e in state.audit]
    check("trail opens with RESOLVED", kinds[0] == "RESOLVED")
    check("records the INTERRUPT", "INTERRUPT" in kinds)
    check("records the ABORT", "ABORTED" in kinds)
    check("records the COMMIT", "COMMITTED" in kinds)
    check("seq is contiguous", [e.seq for e in state.audit] == list(range(len(state.audit))))

    section("7. The commit point is a one-way door")
    check("abort_current() now refuses", agent.abort_current("too late") is False,
          "the message exists; only a supplement is honest now (row R5)")

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
