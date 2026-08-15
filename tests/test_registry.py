"""
Module 1 gate — ten tests.

Six of these come from the master plan. Four more exist because the plan's
Prompt 1 contained instructions that produce defects when followed literally,
and a test is the only thing that stops a future refactor quietly reintroducing
them:

  test_foreign_key_violation_raises_integrity_not_duplicate
  test_pragma_foreign_keys_on_for_every_connection
  test_domain_query_includes_offline_people
  test_infrastructure_matches_exactly_seven

Read the assertions as claims about the SYSTEM, not about the code. Each one
should be a sentence you would be willing to say out loud in a walkthrough.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError
from sqlalchemy import func, insert, select
from sqlalchemy.exc import IntegrityError

from alert_router.db import foreign_keys_enabled
from alert_router.models_orm import ChannelHealth, Evaluation, Stakeholder
from alert_router.registry import DuplicateQueryError
from alert_router.schemas import Availability

# The four shapes Appendix A's scenarios are built on.
PRIYA = "stk-001"      # L3, primary infrastructure, on-call, online  -> 139
TOM = "stk-002"        # L1, primary infrastructure, on-call, online  -> 123
ELENA = "stk-003"      # L5, primary infrastructure, off-call, OFFLINE -> 140
MARCUS = "stk-004"     # L4, SECONDARY infrastructure, online          ->  87


# ─────────────────────────────────────────────────────────────────────────────
# 1 — the seed
# ─────────────────────────────────────────────────────────────────────────────


async def test_seed_loads_ten_stakeholders(session_factory):
    """The registry is populated, and channel_health is populated with it.

    Seeding channel_health up front is what lets Module 3 ask "is a healthy
    fallback available?" with a plain SELECT, instead of every call site having
    to treat a missing row as implicitly healthy.
    """
    async with session_factory() as session:
        people = await session.scalar(select(func.count()).select_from(Stakeholder))
        assert people == 10

        # One healthy row per (person, channel) in that person's channel_order.
        # 25 across the ten seeded people — see data/seed_stakeholders.json.
        health = await session.scalar(select(func.count()).select_from(ChannelHealth))
        assert health == 25
        unhealthy = await session.scalar(
            select(func.count()).select_from(ChannelHealth).where(
                ChannelHealth.healthy == 0
            )
        )
        assert unhealthy == 0, "everyone starts with working transports"

        ids = set((await session.scalars(select(Stakeholder.id))).all())
        assert {PRIYA, TOM, ELENA, MARCUS} <= ids


# ─────────────────────────────────────────────────────────────────────────────
# 2, 3, 4 — the domain query
# ─────────────────────────────────────────────────────────────────────────────


async def test_domain_query_matches_primary_and_secondary(registry, critical_alert):
    """One SELECT, N candidates — and secondary competence counts.

    Marcus Webb's primary domain is networking; infrastructure is secondary for
    him. He must still appear, because ranking is what decides that a
    primary-domain L3 outranks a secondary-domain L4 — not the query.
    """
    snapshots = await registry.query_by_domain(critical_alert)
    ids = {s.stakeholder.id for s in snapshots}

    assert PRIYA in ids, "primary-domain match missing"
    assert MARCUS in ids, "secondary-domain match missing (json_each not working?)"

    fits = {s.stakeholder.id: s.stakeholder.domain_fit("infrastructure") for s in snapshots}
    assert fits[PRIYA] == "primary"
    assert fits[MARCUS] == "secondary"


async def test_domain_query_includes_offline_people(registry, critical_alert):
    """THE PULL MUST NOT FILTER ON AVAILABILITY.

    Adding `WHERE status='online'` looks like an obvious optimisation. It would
    drop Elena Fischer — the offline L5 director — from the ladder entirely, so
    the agent could never escalate UP to her, and invariant I4's entire
    demonstration (matrix row R4) would become unreachable. The failure would
    not surface until Module 4, disguised as a decision bug.

    Availability is a RANKING input, not a MATCHING criterion.
    """
    snapshots = await registry.query_by_domain(critical_alert)
    by_id = {s.stakeholder.id: s for s in snapshots}

    assert ELENA in by_id, "offline stakeholder was filtered out of the domain query"
    assert by_id[ELENA].status is Availability.OFFLINE
    assert by_id[ELENA].status.reachability == 0


async def test_infrastructure_matches_exactly_seven(registry, critical_alert):
    """Seven of ten. The seed is designed around this number.

    Three primary (Priya, Tom, Elena) plus four secondary. The video narration
    says "one query, seven candidates", so this is a spec, not an accident.
    """
    snapshots = await registry.query_by_domain(critical_alert)
    assert len(snapshots) == 7

    primary = [s for s in snapshots if s.stakeholder.domain_fit("infrastructure") == "primary"]
    secondary = [s for s in snapshots if s.stakeholder.domain_fit("infrastructure") == "secondary"]
    assert len(primary) == 3
    assert len(secondary) == 4

    # One round trip, not seven: every snapshot shares one observation instant.
    assert len({s.observed_at for s in snapshots}) == 1


# ─────────────────────────────────────────────────────────────────────────────
# 5, 6 — INVARIANT I3, the query budget
# ─────────────────────────────────────────────────────────────────────────────


async def test_second_pull_same_alert_raises_duplicate_query_error(
    registry, critical_alert
):
    """I3: one availability query per person per alert.

    This is not a rule the code remembers to follow. It is the composite
    primary key of the `evaluations` table, so a second pull is a write the
    database refuses. The exception chains from that IntegrityError, which is
    what keeps the claim checkable rather than merely asserted.
    """
    first = await registry.query_by_domain(critical_alert)
    assert len(first) == 7
    assert await registry.evaluation_count(critical_alert.alert_id) == 7

    with pytest.raises(DuplicateQueryError) as exc_info:
        await registry.query_by_domain(critical_alert)
    assert isinstance(exc_info.value.__cause__, IntegrityError)

    # Targeting one already-evaluated person is refused just as firmly.
    with pytest.raises(DuplicateQueryError):
        await registry.fetch_one(critical_alert, PRIYA)

    # And the failed attempts left the ledger exactly as it was.
    assert await registry.evaluation_count(critical_alert.alert_id) == 7


async def test_different_alert_may_pull_same_person_once(
    registry, critical_alert, stock_alert
):
    """The budget is PER ALERT, not global.

    Two concurrent incidents are each entitled to their own single look at the
    same person. A global counter would starve the second incident of
    information it has every right to — and would be a misreading of the brief.
    """
    await registry.query_by_domain(critical_alert)

    # stock_alert is logistics, so Priya was never matched by ITS domain query.
    fresh = await registry.fetch_one(stock_alert, PRIYA)
    assert fresh.stakeholder.id == PRIYA
    assert fresh.alert_id == stock_alert.alert_id

    # But that alert has now spent its one look at her.
    with pytest.raises(DuplicateQueryError):
        await registry.fetch_one(stock_alert, PRIYA)

    assert await registry.evaluation_count(critical_alert.alert_id) == 7
    assert await registry.evaluation_count(stock_alert.alert_id) == 1


# ─────────────────────────────────────────────────────────────────────────────
# 7, 8 — foreign keys, and the pragma that makes them real
# ─────────────────────────────────────────────────────────────────────────────


async def test_foreign_key_violation_raises_integrity_not_duplicate(
    registry, critical_alert, monkeypatch
):
    """A foreign-key failure must NOT masquerade as a duplicate query.

    The tempting implementation is `except IntegrityError: raise
    DuplicateQueryError`. That single line disguises three unrelated bugs — a
    missing alert row, a CHECK violation, and Module 3's idempotency-key
    collision — as one misleading message, and it is exactly the mistake that
    costs hours of debugging the wrong invariant.

    Here we suppress ensure_alert() to reproduce the missing-parent case
    through the real code path.
    """

    async def _skip_ensure(*_args, **_kwargs) -> None:
        return None

    monkeypatch.setattr(registry, "ensure_alert", _skip_ensure)

    with pytest.raises(IntegrityError) as exc_info:
        await registry.fetch_one(critical_alert, PRIYA)

    assert not isinstance(exc_info.value, DuplicateQueryError)
    assert "FOREIGN KEY" in str(exc_info.value.orig).upper()


async def test_pragma_foreign_keys_on_for_every_connection(session_factory):
    """PRAGMA foreign_keys is per-CONNECTION, not per-database.

    Setting it once inside init_db() would protect one connection and leave
    every other pooled connection unenforced, silently — and the naive test
    (assert on the connection that ran init_db) would pass anyway. So this
    opens two sessions SIMULTANEOUSLY, forcing a second connection out of the
    pool, and asserts on both.
    """
    async with session_factory() as first, session_factory() as second:
        assert await foreign_keys_enabled(first)
        assert await foreign_keys_enabled(second)

        # Prove enforcement, not just the flag: an orphan must be refused.
        with pytest.raises(IntegrityError):
            await second.execute(
                insert(Evaluation).values(
                    alert_id="alr-does-not-exist",
                    stakeholder_id=PRIYA,
                    observed_status="online",
                    observed_at=1.0,
                    qualification=0.0,
                    reachability=2,
                    ladder_rank=-1,
                    source="pull",
                )
            )
        await second.rollback()


# ─────────────────────────────────────────────────────────────────────────────
# 9, 10 — the push channel, and why staleness is the point
# ─────────────────────────────────────────────────────────────────────────────


async def test_push_event_does_not_write_evaluations_row(
    registry, critical_alert, bus
):
    """PUSH IS GENUINELY FREE. This test is the heart of the submission.

    The brief demands the agent detect a mid-flight availability change without
    re-querying anyone already evaluated. That is only satisfiable because a
    presence change ANNOUNCES itself and carries its new state in the payload —
    no question is asked, so nothing is charged.

    The query counter measures questions asked, not facts known.
    """
    await registry.query_by_domain(critical_alert)
    before = await registry.evaluation_count(critical_alert.alert_id)
    assert before == 7

    async with bus.subscribe() as subscription:
        await registry.set_status(PRIYA, Availability.OFFLINE, reason="laptop closed")

        event = await subscription.get(timeout=1.0)
        assert event is not None, "presence change was not published"
        assert event.stakeholder_id == PRIYA
        assert event.previous is Availability.ONLINE
        assert event.current is Availability.OFFLINE
        assert event.went_offline  # the payload carries the new state

    after = await registry.evaluation_count(critical_alert.alert_id)
    assert after == before, "a push event charged the query ledger"

    # Every row in the ledger was paid for by a pull. None came from a push.
    assert all(
        source == "pull"
        for source in await _evaluation_sources(registry, critical_alert.alert_id)
    )


async def test_snapshot_unchanged_after_registry_mutation(
    registry, critical_alert
):
    """THE PRECONDITION FOR THE ENTIRE MID-FLIGHT MECHANISM.

    A CandidateSnapshot is a point-in-time observation, not a view. After the
    registry changes underneath it, the snapshot must still say what it said.

    That staleness is the feature. It is what allows a divergence to exist
    between what the agent believes and what is true — and that divergence IS
    the mid-flight signal. If the snapshot silently refreshed itself, there
    would be no change left to detect and nothing for a push event to tell us.

    The way this breaks in practice is subtle: if the snapshot held a
    still-attached ORM object, reading `.status` would re-query the database and
    quietly return current truth.
    """
    snapshots = await registry.query_by_domain(critical_alert)
    priya = next(s for s in snapshots if s.stakeholder.id == PRIYA)
    assert priya.status is Availability.ONLINE

    await registry.set_status(PRIYA, Availability.OFFLINE, reason="laptop closed")

    # The cached observation has not moved...
    assert priya.status is Availability.ONLINE
    assert priya.stakeholder.status is Availability.ONLINE
    assert priya.source == "pull"

    # ...while ground truth has.
    assert (await registry.peek(PRIYA)).status is Availability.OFFLINE

    # And the model is genuinely immutable, not merely conventionally so.
    # frozen=True is invariant I1 expressed as a type rather than as discipline.
    with pytest.raises(ValidationError):
        priya.status = Availability.OFFLINE


# ─────────────────────────────────────────────────────────────────────────────
# helper
# ─────────────────────────────────────────────────────────────────────────────


async def _evaluation_sources(registry, alert_id: str) -> list[str]:
    async with registry._sessions() as session:  # noqa: SLF001 - test introspection
        return list(
            (
                await session.scalars(
                    select(Evaluation.source).where(Evaluation.alert_id == alert_id)
                )
            ).all()
        )
