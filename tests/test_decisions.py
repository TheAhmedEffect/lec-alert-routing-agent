"""
Module 4 gate — the full truth table, R1 through R11.

NOT ONE OF THESE TESTS TOUCHES A DATABASE. That is the point of keeping
decisions.py pure: states are built from literal records, `decide()` is a plain
function, and the whole eleven-row table runs in milliseconds. Cheap tests are
tests that get run, and a truth table is only trustworthy if every row is
covered — including the boring ones.

The pair that matters most is test_r2 and test_r5: the SAME event, the same
channel, the same person, differing only in whether the commit point has passed.
If the phase predicate is ever dropped, one of them starts returning the other's
answer and an alert quietly under-delivers.
"""

from __future__ import annotations

import re

import pytest

from alert_router.decisions import (
    MATRIX,
    ROW_IDS,
    ChannelFacts,
    NoMatchingRow,
    decide,
)
from alert_router.ranking import build_ladder
from alert_router.schemas import (
    AlertEvent,
    AttemptRecord,
    AttemptState,
    Availability,
    CandidateSnapshot,
    Channel,
    DecisionAction,
    InterruptEvent,
    InterruptKind,
    Severity,
    StakeholderRecord,
)
from alert_router.state import DispatchState

PRIYA, TOM, ELENA, MARCUS, SOFIA = (
    "stk-001",
    "stk-002",
    "stk-003",
    "stk-004",
    "stk-006",
)


# ─────────────────────────────────────────────────────────────────────────────
# Builders — literal records, no database, no fixtures
# ─────────────────────────────────────────────────────────────────────────────


def person(
    person_id: str,
    name: str,
    tier: int,
    *,
    primary: str = "infrastructure",
    secondary: tuple[str, ...] = (),
    on_call: bool = False,
    status: Availability = Availability.ONLINE,
    preferred: Channel = Channel.SLACK,
    fallbacks: tuple[Channel, ...] = (Channel.EMAIL,),
) -> StakeholderRecord:
    return StakeholderRecord(
        id=person_id,
        name=name,
        primary_domain=primary,
        secondary_domains=secondary,
        seniority_tier=tier,
        preferred_channel=preferred,
        fallback_channels=fallbacks,
        status=status,
        on_call=on_call,
    )


def alert_for(severity: Severity = Severity.CRITICAL) -> AlertEvent:
    return AlertEvent(
        alert_id="alr-matrix",
        metric_name="db_replica_lag_seconds",
        value=94.0,
        threshold=30.0,
        severity=severity,
        domain="infrastructure",
        triggered_at=1000.0,
    )


def make_state(people, severity: Severity = Severity.CRITICAL) -> DispatchState:
    """A DispatchState with NO session factory — nothing here writes anywhere."""
    alert = alert_for(severity)
    snapshots = [
        CandidateSnapshot(
            alert_id=alert.alert_id,
            stakeholder=record,
            status=record.status,
            observed_at=1000.0,
        )
        for record in people
    ]
    state = DispatchState.start(alert, snapshots, clock=lambda: 1000.0)
    return state


def attempt_for(
    state: DispatchState,
    person_id: str,
    *,
    channel: Channel = Channel.SLACK,
    attempt_state: AttemptState = AttemptState.IN_FLIGHT,
) -> AttemptRecord:
    record = AttemptRecord(
        alert_id=state.alert.alert_id,
        stakeholder_id=person_id,
        channel=channel,
        role="primary",
        state=attempt_state,
        reserved_at=1000.0,
        committed_at=1001.0 if attempt_state is AttemptState.COMMITTED else None,
    )
    state.register_attempt(record)
    return record


def presence_drop(person_id: str) -> InterruptEvent:
    return InterruptEvent(
        kind=InterruptKind.PRESENCE_CHANGED,
        stakeholder_id=person_id,
        previous=Availability.ONLINE,
        current=Availability.OFFLINE,
        at=1002.0,
    )


def channel_down(person_id: str, channel: Channel) -> InterruptEvent:
    return InterruptEvent(
        kind=InterruptKind.CHANNEL_DEGRADED,
        stakeholder_id=person_id,
        channel=channel,
        healthy=False,
        at=1002.0,
        reason="adapter refused",
    )


def better_match(person_id: str) -> InterruptEvent:
    return InterruptEvent(
        kind=InterruptKind.BETTER_MATCH,
        stakeholder_id=person_id,
        current=Availability.ONLINE,
        at=1002.0,
        reason="now reachable and out-qualifies the incumbent",
    )


#: Appendix A's infrastructure ladder, as literals.
def standard_people():
    return [
        person(PRIYA, "Priya Raman", 3, on_call=True),                    # 139
        person(TOM, "Tom Beckett", 1, on_call=True),                      # 123
        person(ELENA, "Elena Fischer", 5, status=Availability.OFFLINE,
               preferred=Channel.SMS, fallbacks=(Channel.EMAIL,)),        # 140
        person(MARCUS, "Marcus Webb", 4, primary="networking",
               secondary=("infrastructure",)),                            # 87
    ]


# ─────────────────────────────────────────────────────────────────────────────
# R1 — cross-domain noise
# ─────────────────────────────────────────────────────────────────────────────


def test_r1_event_about_someone_who_is_not_the_incumbent_changes_nothing():
    """The cheapest filter in the system, and most of the bus traffic."""
    state = make_state(standard_people())
    attempt_for(state, PRIYA)

    decision = decide(state, presence_drop(SOFIA), state.current_attempt)
    assert decision.matrix_row == "R1"
    assert decision.action is DecisionAction.CONTINUE_UNCHANGED

    # A LADDER MEMBER who is not the incumbent is also R1: their availability
    # changing does not affect a send that is aimed at somebody else.
    assert decide(state, presence_drop(MARCUS), state.current_attempt).matrix_row == "R1"


# ─────────────────────────────────────────────────────────────────────────────
# R2 / R5 — the same event, separated only by phase
# ─────────────────────────────────────────────────────────────────────────────


def test_r2_presence_drop_on_a_persistent_channel_is_a_non_event():
    """SMS waits on the device. Offline means away from keyboard, not
    unreachable — and re-routing here would be pure waste."""
    state = make_state(standard_people())
    attempt = attempt_for(state, PRIYA, channel=Channel.SMS)
    assert attempt.state.is_pre_commit

    decision = decide(state, presence_drop(PRIYA), attempt)
    assert decision.matrix_row == "R2"
    assert decision.action is DecisionAction.CONTINUE_UNCHANGED


def test_r5_the_identical_event_post_commit_escalates_in_parallel():
    """Same person, same channel, same event — the ONLY difference is that the
    commit point has passed. You cannot unsend a message, so the honest response
    is a supplement rather than a retraction."""
    state = make_state(standard_people())
    attempt = attempt_for(
        state, PRIYA, channel=Channel.SMS, attempt_state=AttemptState.COMMITTED
    )
    assert not attempt.state.is_pre_commit

    decision = decide(state, presence_drop(PRIYA), attempt)
    assert decision.matrix_row == "R5"
    assert decision.action is DecisionAction.COMPLETE_AND_ESCALATE_PARALLEL


def test_r2_and_r5_are_separated_by_phase_alone():
    """The regression guard for the whole table.

    If the phase predicate is ever dropped, these two collapse into one answer
    and an alert silently under-delivers. Asserting them side by side with a
    single differing variable makes that impossible to miss.
    """
    people = standard_people()
    event = presence_drop(PRIYA)

    pre = make_state(people)
    pre_attempt = attempt_for(pre, PRIYA, channel=Channel.SMS,
                              attempt_state=AttemptState.IN_FLIGHT)

    post = make_state(people)
    post_attempt = attempt_for(post, PRIYA, channel=Channel.SMS,
                               attempt_state=AttemptState.COMMITTED)

    assert decide(pre, event, pre_attempt).matrix_row == "R2"
    assert decide(post, event, post_attempt).matrix_row == "R5"


# ─────────────────────────────────────────────────────────────────────────────
# R3 / R4 — the floor, and invariant I4
# ─────────────────────────────────────────────────────────────────────────────


def test_r3_presence_drop_on_a_synchronous_channel_reroutes_when_floor_cleared():
    """Appendix A's `reroute`. Tom is on-call at 123 against a floor of 120."""
    state = make_state(standard_people())
    attempt = attempt_for(state, PRIYA, channel=Channel.SLACK)

    decision = decide(state, presence_drop(PRIYA), attempt)
    assert decision.matrix_row == "R3"
    assert decision.action is DecisionAction.ABORT_AND_REROUTE
    assert decision.target_id == TOM
    assert "123" in decision.rationale and "120" in decision.rationale


def test_r4_refuses_to_route_down_and_satisfies_all_three_obligations():
    """INVARIANT I4. Appendix A's `floor` scenario, and the video's centrepiece.

    Tom off the rota scores 108 against a floor of 120. Nobody reachable clears
    it, so the agent refuses to route down — and must do all three things:
    hold the incumbent on a persistent channel, escalate up, and record the
    rejection WITH THE ARITHMETIC.
    """
    people = [
        person(PRIYA, "Priya Raman", 3, on_call=True),                 # 139
        person(TOM, "Tom Beckett", 1, on_call=False),                  # 108
        person(ELENA, "Elena Fischer", 5, status=Availability.OFFLINE,
               preferred=Channel.SMS),                                  # 140
    ]
    state = make_state(people)
    attempt = attempt_for(state, PRIYA, channel=Channel.SLACK)

    decision = decide(
        state,
        presence_drop(PRIYA),
        attempt,
        ChannelFacts(healthy=(Channel.SLACK, Channel.EMAIL), persistent=Channel.EMAIL),
    )

    assert decision.matrix_row == "R4"
    assert decision.action is DecisionAction.HOLD_AND_ESCALATE_UP

    # 1 — the incumbent is kept, on a channel that survives them being offline
    assert decision.target_id == PRIYA
    assert decision.target_channel is Channel.EMAIL
    assert decision.target_channel.is_persistent

    # 2 — the most QUALIFIED member is paged, not the most available one
    assert decision.escalate_to_id == ELENA

    # 3 — the rejection is on record, with the numbers that justified it
    suppressed = dict(decision.suppressed)
    assert TOM in suppressed, "the under-qualified junior was skipped silently"
    assert "108" in suppressed[TOM]
    assert "120" in suppressed[TOM]
    assert "139" in suppressed[TOM]


# ─────────────────────────────────────────────────────────────────────────────
# R6 / R7 — transports fail independently of people
# ─────────────────────────────────────────────────────────────────────────────


def test_r6_channel_degraded_fails_over_without_changing_person():
    """Changing person in response to a transport fault is an over-reaction.

    Same person, same idempotency key, next pipe — one notification.
    """
    state = make_state(standard_people())
    attempt = attempt_for(state, PRIYA, channel=Channel.SLACK)

    decision = decide(
        state,
        channel_down(PRIYA, Channel.SLACK),
        attempt,
        ChannelFacts(healthy=(Channel.EMAIL,), persistent=Channel.EMAIL),
    )
    assert decision.matrix_row == "R6"
    assert decision.action is DecisionAction.CHANNEL_FAILOVER
    assert decision.target_id == PRIYA, "R6 must not change the recipient"
    assert decision.target_channel is Channel.EMAIL


def test_r7_no_healthy_transport_left_reroutes_to_a_different_person():
    """Now they are genuinely unreachable, so changing person IS correct."""
    state = make_state(standard_people())
    attempt = attempt_for(state, PRIYA, channel=Channel.SLACK)

    decision = decide(
        state,
        channel_down(PRIYA, Channel.SLACK),
        attempt,
        ChannelFacts(healthy=(), persistent=None),
    )
    assert decision.matrix_row == "R7"
    assert decision.action is DecisionAction.ABORT_AND_REROUTE


# ─────────────────────────────────────────────────────────────────────────────
# R8 / R9 / R10 — better matches
# ─────────────────────────────────────────────────────────────────────────────


def test_r8_never_renotifies_someone_already_notified():
    """I2 outranks optimality. Never re-notify to improve a decision."""
    state = make_state(standard_people())
    attempt = attempt_for(state, PRIYA)
    state.mark_notified(ELENA)

    decision = decide(state, better_match(ELENA), attempt)
    assert decision.matrix_row == "R8"
    assert decision.action is DecisionAction.CONTINUE_UNCHANGED


def test_r9_escalates_in_parallel_and_the_target_is_a_ladder_member():
    """Elena at 140 out-qualifies Priya at 139 on a CRITICAL alert.

    Different people mean different idempotency keys, so two notifications here
    are two notifications — not a duplicate.
    """
    state = make_state(standard_people())
    attempt = attempt_for(state, PRIYA)

    decision = decide(state, better_match(ELENA), attempt)
    assert decision.matrix_row == "R9"
    assert decision.action is DecisionAction.COMPLETE_AND_ESCALATE_PARALLEL
    assert decision.escalate_to_id == ELENA
    assert state.in_ladder(decision.escalate_to_id), (
        "escalating to a non-member would require a new availability query"
    )
    assert "140" in decision.rationale and "139" in decision.rationale


def test_r9_refuses_a_target_outside_the_ladder():
    """Guardrail: naming a stranger means a new pull, which breaks I3."""
    state = make_state(standard_people())
    attempt = attempt_for(state, PRIYA)

    with pytest.raises(NoMatchingRow, match="not in the ladder"):
        decide(state, better_match("stk-999"), attempt)


def test_r10_low_severity_does_not_wake_a_director():
    """Escalation has a cost, and the cost is a human's attention."""
    state = make_state(standard_people(), severity=Severity.LOW)
    attempt = attempt_for(state, PRIYA)

    decision = decide(state, better_match(ELENA), attempt)
    assert decision.matrix_row == "R10"
    assert decision.action is DecisionAction.CONTINUE_UNCHANGED


# ─────────────────────────────────────────────────────────────────────────────
# R11 and the no-silent-default rule
# ─────────────────────────────────────────────────────────────────────────────


def test_r11_exhaustion_is_terminal_and_loud():
    """A silent drop is the worst possible outcome for an alerting system."""
    state = make_state(standard_people())
    attempt = attempt_for(state, PRIYA, attempt_state=AttemptState.FAILED)
    for person_id in (TOM, ELENA, MARCUS):
        attempt_for(state, person_id, attempt_state=AttemptState.FAILED)

    assert state.remaining == []
    assert not state.notified

    decision = decide(state, channel_down(PRIYA, Channel.SLACK), attempt)
    assert decision.matrix_row == "R11"
    assert decision.action is DecisionAction.EXHAUSTED


def test_unmatched_event_raises_rather_than_defaulting():
    """No silent defaults.

    Falling through to CONTINUE_UNCHANGED would make every future gap in the
    table invisible: the system would quietly do nothing, forever, and no test
    would notice. A missing row is a bug, and bugs should be loud.
    """
    state = make_state(standard_people())
    # A BETTER_MATCH that is not actually better, on a HIGH alert: R8 no,
    # R9 no (not higher), R10 no (not LOW), R11 no (candidates remain).
    attempt = attempt_for(state, ELENA)  # incumbent is the 140
    state.mark_notified("nobody")        # keep R11 from firing

    with pytest.raises(NoMatchingRow):
        decide(state, better_match(MARCUS), attempt)   # Marcus 87 < Elena 140


# ─────────────────────────────────────────────────────────────────────────────
# Table-level invariants
# ─────────────────────────────────────────────────────────────────────────────


def test_every_matrix_row_has_a_test():
    """Coverage as an assertion, not as a hope.

    If someone adds R12 without a test, this fails. That is the whole point of a
    truth table: partial coverage of a first-match-wins sequence is worse than
    none, because the untested rows are exactly the shadowed ones.
    """
    tested = set()
    for name in globals():
        match = re.match(r"test_(r\d+)[_a-z]*", name)
        if match:
            tested.add(match.group(1).upper())
    missing = set(ROW_IDS) - tested
    assert not missing, f"matrix rows with no test: {sorted(missing)}"


def test_matrix_order_is_the_specification():
    """R1..R11, contiguous, in order. The ORDER is the spec — R3 shadows R4 by
    design, and any reshuffle silently changes behaviour."""
    assert list(ROW_IDS) == [f"R{i}" for i in range(1, 12)]
    assert len(MATRIX) == 11


def test_every_decision_carries_its_matrix_row():
    """Untagged decisions make the audit trail untraceable, and the walkthrough
    loses its best move: pointing at a line and saying 'that row fired'."""
    state = make_state(standard_people())
    attempt = attempt_for(state, PRIYA, channel=Channel.SLACK)

    for event in (presence_drop(SOFIA), presence_drop(PRIYA), better_match(ELENA)):
        decision = decide(
            state,
            event,
            attempt,
            ChannelFacts(healthy=(Channel.SLACK,), persistent=Channel.EMAIL),
        )
        assert decision.matrix_row in ROW_IDS
        assert decision.rationale, "a decision with no rationale is unreviewable"
