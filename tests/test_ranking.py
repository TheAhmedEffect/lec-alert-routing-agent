"""
Module 2 gate — twelve tests.

The two that matter most are test_offline_director_beats_online_junior and
test_qualification_ignores_availability. Between them they pin invariant I4 from
both directions: the director really is more qualified, and no amount of
presence can change anyone's qualification at all.

Read the ladder these tests assert against:

    Priya  (reach 2, qual 139)   <- contacted first
    Tom    (reach 2, qual 123)
    Daniel (reach 2, qual  94)
    Marcus (reach 2, qual  87)
    Aisha  (reach 2, qual  86)
    Yuki   (reach 2, qual  71)
    Elena  (reach 0, qual 140)   <- LAST, and the most qualified person in it

Elena being last is not a bug and not a compromise. Ordering for first contact
and ranking for competence are different questions, and this module answers them
with different functions.
"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import func, select

from alert_router import config
from alert_router.models_orm import Evaluation
from alert_router.ranking import (
    best_by_qualification,
    build_ladder,
    clears_floor,
    floor_for,
    score,
)
from alert_router.schemas import (
    AttemptRecord,
    AttemptState,
    Availability,
    Channel,
    InterruptEvent,
    InterruptKind,
    Severity,
)
from alert_router.state import DispatchState, persist_ladder

PRIYA = "stk-001"      # L3 primary infra, on-call, online   -> 139
TOM = "stk-002"        # L1 primary infra, on-call, online   -> 123
ELENA = "stk-003"      # L5 primary infra, off-call, OFFLINE -> 140
MARCUS = "stk-004"     # L4 SECONDARY infra, online          ->  87
SOFIA = "stk-006"      # security only — never in this ladder


def by_id(plan, stakeholder_id):
    return next(c for c in plan.ladder if c.snapshot.stakeholder.id == stakeholder_id)


# ─────────────────────────────────────────────────────────────────────────────
# 1, 2 — scoring
# ─────────────────────────────────────────────────────────────────────────────


def test_primary_domain_l3_outranks_secondary_domain_l4(plan):
    """Domain fit dominates seniority.

    Priya is two tiers junior to Marcus and scores 52 points higher, because
    specialism is worth 45 points more than adjacency and rank only moves 8
    points per tier. That gap is a deliberate design choice: the person who
    knows the system beats the person who outranks them.
    """
    priya = by_id(plan, PRIYA)
    marcus = by_id(plan, MARCUS)

    assert priya.score.qualification == 139  # 100 + 24 + 15
    assert marcus.score.qualification == 87  # 55 + 32 + 0
    assert priya.score.qualification > marcus.score.qualification
    assert priya.snapshot.stakeholder.seniority_tier < marcus.snapshot.stakeholder.seniority_tier


def test_offline_director_beats_online_junior_on_qualification(plan):
    """THIS IS INVARIANT I4, and it is asserted as TWO SEPARATE CLAIMS.

    Claim 1 is about COMPETENCE: Elena (140) is more qualified than Tom (123),
    and being offline does not change that by a single point.

    Claim 2 is about CONTACT ORDER: Tom sorts above Elena anyway, because the
    ladder's first key is reachability and an unreachable person cannot answer a
    synchronous page.

    These must never be collapsed into one assertion. A test written as "the
    director outranks the junior" against sort ORDER would fail, and the obvious
    fix — reordering the sort key — would destroy I4 entirely by letting
    presence decide competence.
    """
    elena = by_id(plan, ELENA)
    tom = by_id(plan, TOM)

    # Claim 1 — competence. Availability is nowhere in these numbers.
    assert elena.score.qualification == 140
    assert tom.score.qualification == 123
    assert elena.score.qualification > tom.score.qualification

    # Claim 2 — contact order, a different question with a different answer.
    assert elena.score.reachability == 0
    assert tom.score.reachability == 2
    assert tom.rank < elena.rank, "reachable candidates are contacted first"
    assert plan.ladder[-1].snapshot.stakeholder.id == ELENA
    assert plan.ladder[0].snapshot.stakeholder.id == PRIYA

    # And the most QUALIFIED candidate is not the first one — row R4 needs this.
    assert best_by_qualification(plan.ladder).snapshot.stakeholder.id == ELENA


def test_qualification_ignores_availability(snapshots, critical_alert):
    """Score the same person twice, online and offline. Only reachability moves.

    This is I4 from the other direction: not "the ranking happens to come out
    right", but "presence is arithmetically incapable of affecting competence".
    """
    priya = next(s for s in snapshots if s.stakeholder.id == PRIYA)
    assert priya.status is Availability.ONLINE

    online_score = score(priya, critical_alert)
    offline_score = score(
        priya.model_copy(update={"status": Availability.OFFLINE}), critical_alert
    )

    assert online_score.qualification == offline_score.qualification == 139
    assert online_score.domain_points == offline_score.domain_points
    assert online_score.seniority_points == offline_score.seniority_points
    assert online_score.on_call_points == offline_score.on_call_points

    # The ONLY difference the two scores are permitted to have.
    assert online_score.reachability == 2
    assert offline_score.reachability == 0


# ─────────────────────────────────────────────────────────────────────────────
# 3, 4 — the floor
# ─────────────────────────────────────────────────────────────────────────────


def test_clears_floor_rejects_underqualified_with_numeric_reason(plan, snapshots, critical_alert):
    """The `floor` scenario. Tom off-call scores 108 against a floor of 120.

    The reason string is not decoration — it goes into the audit trail, into the
    notification, and onto the screen in the walkthrough. It must contain the
    arithmetic, because the arithmetic IS the argument.
    """
    priya = by_id(plan, PRIYA)

    # Take Tom off the rota, exactly as the `floor` scenario does.
    tom_snapshot = next(s for s in snapshots if s.stakeholder.id == TOM)
    off_rota = tom_snapshot.model_copy(
        update={"stakeholder": tom_snapshot.stakeholder.model_copy(update={"on_call": False})}
    )
    tom_off_rota = build_ladder([off_rota], critical_alert).ladder[0]

    assert tom_off_rota.score.qualification == 108  # 100 + 8 + 0
    assert floor_for(priya.score.qualification, Severity.CRITICAL) == 120

    ok, reason = clears_floor(tom_off_rota, priya, Severity.CRITICAL)
    assert ok is False
    assert "108" in reason and "120" in reason
    assert "139" in reason, "the incumbent's score justifies the floor; show it"
    assert "CRITICAL" in reason


def test_clears_floor_accepts_when_within_tolerance(plan):
    """The `reroute` scenario. Tom on-call scores 123 and clears the floor of 120.

    123 is below Priya's 139 — a real downgrade — but within DOWNGRADE_TOLERANCE
    and above the CRITICAL minimum. People are not interchangeable and some slack
    is realistic; the floor bounds how much.
    """
    priya = by_id(plan, PRIYA)
    tom = by_id(plan, TOM)

    ok, reason = clears_floor(tom, priya, Severity.CRITICAL)
    assert ok is True
    assert "123" in reason and "120" in reason

    # ...and the floor is what does the work: on a LOW alert the minimum is 0,
    # so the incumbent term binds instead.
    assert floor_for(priya.score.qualification, Severity.LOW) == 114
    assert floor_for(priya.score.qualification, Severity.CRITICAL) == 120
    assert floor_for(None, Severity.CRITICAL) == config.MIN_QUALIFICATION["critical"]


def test_ineligible_candidates_are_excluded_from_the_ladder(snapshots, critical_alert):
    """Someone with no domain overlap is not a weak candidate. They are not a
    candidate — DOMAIN_POINTS['none'] is None, a sentinel meaning ineligible."""
    plan = build_ladder(snapshots, critical_alert)
    assert SOFIA not in {c.snapshot.stakeholder.id for c in plan.ladder}
    assert all(c.score.eligible for c in plan.ladder)
    assert len(plan.ladder) == 7
    assert [c.rank for c in plan.ladder] == list(range(7))


# ─────────────────────────────────────────────────────────────────────────────
# 5, 6, 7 — DispatchState
# ─────────────────────────────────────────────────────────────────────────────


def test_next_candidate_skips_aborted_attempts(state):
    """An ABORTED attempt still blocks the person forever.

    The idempotency key was taken at reservation, so re-offering them would
    surface in Module 3 as an IntegrityError mid-reroute — an error that looks
    like a duplicate bug but is really a ladder-walk bug. Skipping only
    `notified` here is how that happens.
    """
    assert state.next_candidate().snapshot.stakeholder.id == PRIYA

    state.register_attempt(
        AttemptRecord(
            alert_id=state.alert.alert_id,
            stakeholder_id=PRIYA,
            channel=Channel.SLACK,
            role="primary",
            state=AttemptState.ABORTED,
            reserved_at=1.0,
            outcome_reason="recipient went offline mid-send",
        )
    )
    assert PRIYA not in state.notified          # nothing was ever delivered
    assert PRIYA in state.attempted             # but we did try
    assert state.next_candidate().snapshot.stakeholder.id == TOM

    # Suppression also removes someone from the walk, with its reason preserved.
    state.mark_suppressed(TOM, "qualification 108 < floor 120")
    assert state.next_candidate().snapshot.stakeholder.id not in (PRIYA, TOM)
    assert "108" in state.suppressed[TOM]


def test_patch_from_push_returns_false_for_unevaluated_subject(state):
    """A stranger's presence change is not ours to act on.

    Cross-domain traffic constantly names people this alert never evaluated.
    Indexing blindly would raise KeyError on entirely normal events; returning
    False is decision-matrix row R1 arriving early.
    """
    stranger = InterruptEvent(
        kind=InterruptKind.PRESENCE_CHANGED,
        stakeholder_id=SOFIA,
        previous=Availability.BUSY,
        current=Availability.OFFLINE,
        at=1.0,
    )
    assert state.patch_from_push(stranger) is False
    assert SOFIA not in state.observed

    ours = InterruptEvent(
        kind=InterruptKind.PRESENCE_CHANGED,
        stakeholder_id=PRIYA,
        previous=Availability.ONLINE,
        current=Availability.OFFLINE,
        at=2.0,
    )
    assert state.patch_from_push(ours) is True
    assert state.observed[PRIYA].status is Availability.OFFLINE
    assert state.observed[PRIYA].source == "push"


def test_rescore_does_not_change_ladder_membership_or_rank(state):
    """Re-scoring is legal; re-resolving is not.

    Re-scoring recomputes from observations already paid for. Adding a member
    would require a new pull, so the method structurally cannot do it — it walks
    the existing ladder and preserves each rank.
    """
    before_ids = [c.snapshot.stakeholder.id for c in state.plan.ladder]
    before_ranks = [c.rank for c in state.plan.ladder]

    state.patch_from_push(
        InterruptEvent(
            kind=InterruptKind.PRESENCE_CHANGED,
            stakeholder_id=PRIYA,
            previous=Availability.ONLINE,
            current=Availability.OFFLINE,
            at=2.0,
        )
    )
    plan = state.rescore()

    assert [c.snapshot.stakeholder.id for c in plan.ladder] == before_ids
    assert [c.rank for c in plan.ladder] == before_ranks
    assert len(state.evaluated) == 7, "re-scoring must not spend query budget"


def test_rescore_after_push_moves_reachability_but_not_qualification(state):
    """Invariant I4, visible in the diff of a single operation.

    Priya drops offline. Her reachability goes 2 -> 0. Her qualification does not
    move by a single point, because qualification has no availability term. If
    this test ever fails, someone has added one.
    """
    before = by_id(state.plan, PRIYA).score
    assert (before.qualification, before.reachability) == (139, 2)

    state.patch_from_push(
        InterruptEvent(
            kind=InterruptKind.PRESENCE_CHANGED,
            stakeholder_id=PRIYA,
            previous=Availability.ONLINE,
            current=Availability.OFFLINE,
            at=2.0,
        )
    )
    after = by_id(state.rescore(), PRIYA).score

    assert after.qualification == before.qualification == 139
    assert after.reachability == 0

    # And she is STILL more qualified than Tom, who is still online.
    assert after.qualification > by_id(state.plan, TOM).score.qualification


# ─────────────────────────────────────────────────────────────────────────────
# 8, 9 — audit and persistence
# ─────────────────────────────────────────────────────────────────────────────


async def test_audit_seq_is_monotonic_under_concurrent_writers(offline_state):
    """`seq` must be allocated inside the lock.

    audit_events carries UNIQUE(alert_id, seq). In Module 3 the executor and the
    interrupt listener both write here; an unlocked increment would let two
    writers claim the same number and take the dispatch down at the worst moment.
    Here the failure is cheap to reproduce and free to fix.
    """
    writers = 25
    await asyncio.gather(
        *(
            offline_state.record_audit("INTERRUPT", f"writer-{i}", f"event {i}")
            for i in range(writers)
        )
    )

    sequences = sorted(event.seq for event in offline_state.audit)
    assert sequences == list(range(writers)), "duplicate or missing seq"
    assert len(offline_state.audit) == writers
    assert len(offline_state.audit_lines()) == writers


async def test_record_audit_writes_a_row_and_never_updates(state, session_factory):
    """The audit trail is append-only. This is the storage half of invariant I1."""
    from alert_router.models_orm import AuditEvent as AuditEventRow

    await state.record_audit("RESOLVED", "registry", "7 candidates from one query")
    await state.record_audit("RANKED", "ranking", "Priya 139 leads on reachability")

    async with session_factory() as session:
        rows = (
            await session.scalars(
                select(AuditEventRow).order_by(AuditEventRow.seq)
            )
        ).all()

    assert [r.seq for r in rows] == [0, 1]
    assert [r.kind for r in rows] == ["RESOLVED", "RANKED"]
    assert "7 candidates" in rows[0].summary


async def test_persist_ladder_updates_sentinels_without_inserting(
    state, session_factory, critical_alert
):
    """The write-back must be an UPDATE.

    Module 1's pull already created these rows with qualification=0.0 and
    ladder_rank=-1. An INSERT here would collide with the composite primary key
    and raise DuplicateQueryError, which would look like an invariant I3
    violation when it is really a write-back bug — an expensive misdiagnosis.
    """
    async with session_factory() as session:
        before = await session.scalar(
            select(func.count()).select_from(Evaluation).where(
                Evaluation.alert_id == critical_alert.alert_id
            )
        )
    assert before == 7

    written = await persist_ladder(session_factory, state.plan)
    assert written == 7

    async with session_factory() as session:
        rows = (
            await session.scalars(
                select(Evaluation)
                .where(Evaluation.alert_id == critical_alert.alert_id)
                .order_by(Evaluation.ladder_rank)
            )
        ).all()
        after = await session.scalar(
            select(func.count()).select_from(Evaluation).where(
                Evaluation.alert_id == critical_alert.alert_id
            )
        )

    assert after == before, "persist_ladder inserted rows instead of updating them"
    assert all(row.ladder_rank != config.LADDER_RANK_SENTINEL for row in rows)
    assert all(row.source == "pull" for row in rows), "ranking is not a new observation"

    scores = {row.stakeholder_id: row.qualification for row in rows}
    assert scores[PRIYA] == 139
    assert scores[TOM] == 123
    assert scores[ELENA] == 140

    # Rank order in the database matches the frozen ladder.
    assert rows[0].stakeholder_id == PRIYA
    assert rows[-1].stakeholder_id == ELENA
