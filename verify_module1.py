"""
Gate 1 verification — run this yourself and read the output.

This is the manual database inspection from the plan's post-flight checklist,
automated so it is repeatable and so its output can go on screen in the video.
It proves things the test suite asserts, but proves them against a REAL database
file you can also open by hand, and it prints the actual DDL SQLite stored
rather than trusting that create_all() emitted what we asked for.

    python verify_module1.py

Uses its own database file so it can never collide with the demo database.
Scratch tooling: delete it after Gate 1, or keep it and add it to .gitignore.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from sqlalchemy import func, select, text

from alert_router.db import (
    build_engine,
    build_session_factory,
    dump_schema,
    foreign_keys_enabled,
    init_db,
)
from alert_router.models_orm import ChannelHealth, Evaluation, Stakeholder
from alert_router.registry import DuplicateQueryError, PresenceBus, Registry, zero_latency
from alert_router.schemas import AlertEvent, Availability, Severity

DB_FILE = Path(__file__).resolve().parent / "verify_module1.db"
RESULTS: list[bool] = []


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

    # ── 1. the constraints actually reached the database ────────────────────
    section("1. Schema — what SQLite actually stored")
    schema = await dump_schema(engine)

    required = {
        "evaluations": [
            ("composite PRIMARY KEY (I3)", "PRIMARY KEY (alert_id, stakeholder_id)"),
            ("CHECK observed_status", "CHECK (observed_status IN"),
            ("CHECK source", "CHECK (source IN"),
            ("CHECK reachability", "CHECK (reachability BETWEEN 0 AND 2"),
            ("FK -> alerts", "FOREIGN KEY(alert_id) REFERENCES alerts"),
        ],
        "dispatch_attempts": [
            ("UNIQUE idempotency_key (I2)", "UNIQUE (idempotency_key)"),
            ("CHECK state", "CHECK (state IN"),
            ("CHECK role", "CHECK (role IN"),
            ("CHECK commit_time pairing", "committed_at IS NOT NULL"),
        ],
        "stakeholders": [
            ("CHECK seniority_tier", "CHECK (seniority_tier BETWEEN 1 AND 5"),
            ("CHECK status", "CHECK (status IN"),
            ("CHECK preferred_channel", "CHECK (preferred_channel IN"),
        ],
        "audit_events": [
            ("UNIQUE (alert_id, seq)", "UNIQUE (alert_id, seq)"),
        ],
    }

    for table, expectations in required.items():
        ddl = " ".join(schema.get(table, "").split())  # normalise whitespace
        for label, needle in expectations:
            check(f"{table}: {label}", " ".join(needle.split()) in ddl)

    # ── 2. the pragma, on more than one connection ──────────────────────────
    section("2. PRAGMA foreign_keys — on EVERY connection, not just the first")
    async with sessions() as first, sessions() as second:
        check("connection 1 enforces foreign keys", await foreign_keys_enabled(first))
        check("connection 2 enforces foreign keys", await foreign_keys_enabled(second))
        mode = await second.scalar(text("PRAGMA journal_mode"))
        check("journal_mode is WAL", str(mode).lower() == "wal", str(mode))

    # ── 3. the seed ─────────────────────────────────────────────────────────
    section("3. Seed data")
    async with sessions() as session:
        people = await session.scalar(select(func.count()).select_from(Stakeholder))
        health = await session.scalar(select(func.count()).select_from(ChannelHealth))
        check("10 stakeholders", people == 10, str(people))
        check("channel_health seeded", health == 25, f"{health} rows")

        infra = await session.scalar(
            text(
                "SELECT count(*) FROM stakeholders WHERE primary_domain = 'infrastructure' "
                "OR EXISTS (SELECT 1 FROM json_each(secondary_domains) "
                "WHERE json_each.value = 'infrastructure')"
            )
        )
        check("infrastructure matches exactly 7", infra == 7, str(infra))

    # ── 4. I3, live ─────────────────────────────────────────────────────────
    section("4. Invariant I3 — the query budget, demonstrated")
    bus = PresenceBus()
    registry = Registry(sessions, bus=bus, latency=zero_latency)
    alert = AlertEvent(
        alert_id="alr-verify",
        metric_name="db_replica_lag_seconds",
        value=94.0,
        threshold=30.0,
        severity=Severity.CRITICAL,
        domain="infrastructure",
    )

    snapshots = await registry.query_by_domain(alert)
    print(f"         one query returned {len(snapshots)} candidates:")
    for snap in snapshots:
        fit = snap.stakeholder.domain_fit("infrastructure")
        print(
            f"           {snap.stakeholder.name:<17} L{snap.stakeholder.seniority_tier} "
            f"{fit:<9} on_call={str(snap.stakeholder.on_call):<5} {snap.status.value}"
        )
    check("7 candidates from ONE round trip", len(snapshots) == 7)
    check("offline director included", any(s.status is Availability.OFFLINE for s in snapshots))
    check("ledger holds 7 rows", await registry.evaluation_count(alert.alert_id) == 7)

    try:
        await registry.query_by_domain(alert)
        check("second pull refused by the database", False, "IT WAS ALLOWED")
    except DuplicateQueryError as exc:
        cause = type(exc.__cause__).__name__
        check("second pull refused by the database", True, f"chained from {cause}")
        print(f"           {str(exc.__cause__.orig)[:96]}")

    # ── 5. push is free ─────────────────────────────────────────────────────
    section("5. Push — knowledge changes, the query counter does not")
    priya = next(s for s in snapshots if s.stakeholder.id == "stk-001")
    before = await registry.evaluation_count(alert.alert_id)

    async with bus.subscribe() as subscription:
        await registry.set_status("stk-001", Availability.OFFLINE, reason="laptop closed")
        event = await subscription.get(timeout=1.0)

    after = await registry.evaluation_count(alert.alert_id)
    check("event delivered", event is not None)
    check(
        "payload carried the new state",
        event is not None and event.current is Availability.OFFLINE,
        f"{event.previous.value} -> {event.current.value}" if event else "",
    )
    check("query counter did NOT move", before == after, f"{before} -> {after}")
    check("cached snapshot still says online", priya.status is Availability.ONLINE)
    check(
        "ground truth says offline",
        (await registry.peek("stk-001")).status is Availability.OFFLINE,
    )
    print("         ^ that gap between the last two lines IS the mid-flight signal")

    async with sessions() as session:
        sources = set(
            (
                await session.scalars(
                    select(Evaluation.source).where(Evaluation.alert_id == alert.alert_id)
                )
            ).all()
        )
    check("every ledger row says 'pull'", sources == {"pull"}, str(sources))

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
