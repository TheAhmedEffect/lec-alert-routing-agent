"""
Gate 2 verification — run this yourself and read the output.

Prints the ladder with every score term broken out, the floor arithmetic for
both branches of Appendix A, and proof that the Module 1 sentinels were updated
rather than re-inserted.

    python verify_module2.py

The ladder table this prints is the one you will have on screen at 0:20 in the
walkthrough video, so it is worth looking at properly.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from sqlalchemy import func, select

from alert_router import config
from alert_router.db import build_engine, build_session_factory, init_db
from alert_router.models_orm import Evaluation
from alert_router.ranking import (
    best_by_qualification,
    build_ladder,
    clears_floor,
    floor_for,
)
from alert_router.registry import PresenceBus, Registry, zero_latency
from alert_router.schemas import (
    AlertEvent,
    AttemptRecord,
    AttemptState,
    Availability,
    Channel,
    InterruptEvent,
    InterruptKind,
    Severity,
)
from alert_router.state import DispatchState, persist_ladder

DB_FILE = Path(__file__).resolve().parent / "verify_module2.db"
RESULTS: list[bool] = []

PRIYA, TOM, ELENA = "stk-001", "stk-002", "stk-003"


def check(label: str, ok: bool, detail: str = "") -> None:
    RESULTS.append(ok)
    print(f"  [{'PASS' if ok else 'FAIL'}] {label:<52} {detail}")


def section(title: str) -> None:
    print(f"\n{title}\n{'-' * len(title)}")


async def main() -> int:
    for suffix in ("", "-wal", "-shm"):
        Path(str(DB_FILE) + suffix).unlink(missing_ok=True)

    engine = build_engine(f"sqlite+aiosqlite:///{DB_FILE.as_posix()}")
    sessions = build_session_factory(engine)
    await init_db(engine)

    bus = PresenceBus()
    registry = Registry(sessions, bus=bus, latency=zero_latency)
    alert = AlertEvent(
        alert_id="alr-verify-2",
        metric_name="db_replica_lag_seconds",
        value=94.0,
        threshold=30.0,
        severity=Severity.CRITICAL,
        domain="infrastructure",
    )

    snapshots = await registry.query_by_domain(alert)
    state = DispatchState.start(alert, snapshots, session_factory=sessions)
    plan = state.plan

    # ── 1. the ladder ───────────────────────────────────────────────────────
    section("1. The ladder — one query, seven candidates, two independent axes")
    print(f"  {'rank':<5}{'name':<17}{'fit':<11}{'tier':<6}{'on-call':<9}"
          f"{'status':<9}{'reach':<7}{'QUAL':>5}")
    print(f"  {'-' * 74}")
    for candidate in plan.ladder:
        person = candidate.snapshot.stakeholder
        print(
            f"  {candidate.rank:<5}{person.name:<17}"
            f"{person.domain_fit(alert.domain):<11}L{person.seniority_tier:<5}"
            f"{str(person.on_call):<9}{candidate.snapshot.status.value:<9}"
            f"{candidate.score.reachability:<7}{candidate.score.qualification:>5.0f}"
        )

    priya = state.candidate_for(PRIYA)
    tom = state.candidate_for(TOM)
    elena = state.candidate_for(ELENA)

    check("7 eligible candidates", len(plan.ladder) == 7, str(len(plan.ladder)))
    check("Priya 139", priya.score.qualification == 139)
    check("Tom 123", tom.score.qualification == 123)
    check("Elena 140", elena.score.qualification == 140)
    check("Priya is contacted first", plan.ladder[0].snapshot.stakeholder.id == PRIYA)
    check("Elena is contacted LAST", plan.ladder[-1].snapshot.stakeholder.id == ELENA)
    check(
        "yet Elena is the MOST QUALIFIED",
        best_by_qualification(plan.ladder).snapshot.stakeholder.id == ELENA,
        "140 > 139 — she is last only because she is offline",
    )

    # ── 2. the floor, both branches ─────────────────────────────────────────
    section("2. The floor — invariant I4's enforcement point")
    print(f"  CRITICAL minimum      : {config.MIN_QUALIFICATION['critical']:.0f}")
    print(f"  incumbent (Priya)     : {priya.score.qualification:.0f}")
    print(f"  downgrade tolerance   : {config.DOWNGRADE_TOLERANCE:.0f}")
    print(f"  => floor              : "
          f"{floor_for(priya.score.qualification, Severity.CRITICAL):.0f}\n")

    ok_reroute, reason_reroute = clears_floor(tom, priya, Severity.CRITICAL)
    print(f"  scenario 'reroute' : {reason_reroute}")
    check("Tom on-call (123) clears the floor -> R3", ok_reroute)

    # The `floor` scenario: take Tom off the rota.
    await registry.set_on_call(TOM, False)
    off_rota = tom.snapshot.model_copy(
        update={"stakeholder": tom.snapshot.stakeholder.model_copy(
            update={"on_call": False})}
    )
    tom_off = build_ladder([off_rota], alert).ladder[0]
    ok_floor, reason_floor = clears_floor(tom_off, priya, Severity.CRITICAL)
    print(f"  scenario 'floor'   : {reason_floor}")
    check("Tom off-call (108) is REFUSED -> R4", not ok_floor)
    check(
        "the reason carries the arithmetic",
        all(token in reason_floor for token in ("108", "120", "139")),
    )
    await registry.set_on_call(TOM, True)

    # ── 3. push moves reachability, never qualification ─────────────────────
    section("3. A presence drop — what it changes, and what it cannot")
    before = state.candidate_for(PRIYA).score
    state.patch_from_push(
        InterruptEvent(
            kind=InterruptKind.PRESENCE_CHANGED,
            stakeholder_id=PRIYA,
            previous=Availability.ONLINE,
            current=Availability.OFFLINE,
            at=1.0,
        )
    )
    state.rescore()
    after = state.candidate_for(PRIYA).score
    print(f"  Priya reachability : {before.reachability} -> {after.reachability}")
    print(f"  Priya qualification: {before.qualification:.0f} -> "
          f"{after.qualification:.0f}   <- unchanged, and that is invariant I4")
    check("reachability fell to 0", after.reachability == 0)
    check("qualification did NOT move", after.qualification == before.qualification)
    check("ladder membership unchanged", len(state.plan.ladder) == 7)
    check("query budget unchanged", await registry.evaluation_count(alert.alert_id) == 7)

    # ── 4. the write-back ───────────────────────────────────────────────────
    section("4. Sentinel write-back — UPDATE, never INSERT")
    async with sessions() as session:
        before_rows = await session.scalar(
            select(func.count()).select_from(Evaluation).where(
                Evaluation.alert_id == alert.alert_id
            )
        )

    await persist_ladder(sessions, state.plan)

    async with sessions() as session:
        after_rows = await session.scalar(
            select(func.count()).select_from(Evaluation).where(
                Evaluation.alert_id == alert.alert_id
            )
        )
        rows = (
            await session.scalars(
                select(Evaluation)
                .where(Evaluation.alert_id == alert.alert_id)
                .order_by(Evaluation.ladder_rank)
            )
        ).all()

    check("row count unchanged", before_rows == after_rows, f"{before_rows} -> {after_rows}")
    check(
        "no sentinel ranks remain",
        all(r.ladder_rank != config.LADDER_RANK_SENTINEL for r in rows),
    )
    check("every row still says 'pull'", all(r.source == "pull" for r in rows))
    scores = {r.stakeholder_id: r.qualification for r in rows}
    check("Priya persisted at 139", scores[PRIYA] == 139)
    check("Elena persisted at 140", scores[ELENA] == 140)

    # ── 5. the ladder walk ──────────────────────────────────────────────────
    section("5. The ladder walk — aborted attempts are terminal")
    check("first candidate is Priya", state.next_candidate().snapshot.stakeholder.id == PRIYA)
    state.register_attempt(
        AttemptRecord(
            alert_id=alert.alert_id,
            stakeholder_id=PRIYA,
            channel=Channel.SLACK,
            role="primary",
            state=AttemptState.ABORTED,
            reserved_at=1.0,
            outcome_reason="went offline mid-send",
        )
    )
    check("after abort, next is Tom", state.next_candidate().snapshot.stakeholder.id == TOM)
    check("Priya never entered `notified`", PRIYA not in state.notified)

    state.mark_suppressed(TOM, reason_floor)
    nxt = state.next_candidate()
    check("suppressed candidates are skipped too", nxt.snapshot.stakeholder.id not in (PRIYA, TOM))
    print(f"         suppression on record: {state.suppressed[TOM][:78]}")

    bus.close()
    await engine.dispose()
    for suffix in ("", "-wal", "-shm"):
        Path(str(DB_FILE) + suffix).unlink(missing_ok=True)

    print(f"\n{'=' * 72}")
    print(f"  {sum(RESULTS)}/{len(RESULTS)} checks passed")
    print(f"{'=' * 72}")
    return 0 if all(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
