"""
Module 3 gate — the commit point, the abort window, and the interrupt path.

Every test here drives phase transitions with an explicit gate rather than with
sleeps. That is not fastidiousness: a suite that races the scheduler passes
nineteen times and fails the twentieth, and the twentieth is the run you record.

The two tests that carry the most weight:

  test_abort_pre_commit_writes_aborted_and_sends_nothing
      the abort window is real — nothing left the building

  test_shielded_commit_lands_even_when_cancelled_mid_write
      the door is one-way — once committed, cancellation cannot unsend it
"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import func, select

from alert_router.agent import AlertAgent
from alert_router.channels import ChannelConnectError
from alert_router.executor import (
    DispatchExecutor,
    DuplicateDispatchError,
    IllegalTransition,
    PhaseHooks,
)
from alert_router.interrupts import InterruptListener
from alert_router.models_orm import DispatchAttempt, Evaluation
from alert_router.schemas import (
    AttemptState,
    Availability,
    Channel,
    InterruptEvent,
    InterruptKind,
)

PRIYA, TOM, ELENA, SOFIA = "stk-001", "stk-002", "stk-003", "stk-006"
TIMEOUT = 5.0


async def attempt_rows(session_factory, alert_id: str):
    async with session_factory() as session:
        return (
            await session.scalars(
                select(DispatchAttempt)
                .where(DispatchAttempt.alert_id == alert_id)
                .order_by(DispatchAttempt.id)
            )
        ).all()


# ─────────────────────────────────────────────────────────────────────────────
# 1, 2 — the happy path and the reservation ordering
# ─────────────────────────────────────────────────────────────────────────────


async def test_happy_path_reaches_committed(executor, state, bank, session_factory):
    attempt = await executor.dispatch(state, state.next_candidate())

    assert attempt.state is AttemptState.COMMITTED
    assert attempt.committed_at is not None
    assert PRIYA in state.notified
    assert bank.delivered and bank.delivered[0][1] == PRIYA

    rows = await attempt_rows(session_factory, state.alert.alert_id)
    assert len(rows) == 1
    assert rows[0].state == "committed"
    assert rows[0].committed_at is not None
    assert rows[0].idempotency_key == f"{state.alert.alert_id}:{PRIYA}"


async def test_reserve_row_exists_before_send_starts(
    session_factory, state, bank, gate, clock
):
    """The idempotency key is claimed by the DECISION to try, not by delivery.

    Reserve late and invariant I2 has a window in which two attempts can both
    believe they are first. This test parks the executor at in-flight — after
    reserve, before send lands — and proves the row is already there.
    """
    executor = DispatchExecutor(
        session_factory, bank, clock=clock, hooks=PhaseHooks(on_in_flight=gate)
    )
    task = asyncio.create_task(executor.dispatch(state, state.next_candidate()))
    await gate.wait()

    rows = await attempt_rows(session_factory, state.alert.alert_id)
    assert len(rows) == 1, "the reserve row must exist before the send completes"
    assert rows[0].state == "in_flight"
    assert rows[0].committed_at is None, "the commit-time CHECK constraint"
    assert bank.delivered == [], "nothing has been transmitted yet"

    gate.open()
    await asyncio.wait_for(task, TIMEOUT)


# ─────────────────────────────────────────────────────────────────────────────
# 3, 4, 5 — the abort window and the one-way door
# ─────────────────────────────────────────────────────────────────────────────


async def test_abort_pre_commit_writes_aborted_and_sends_nothing(
    session_factory, state, bank, gate, clock
):
    """THE ABORT WINDOW. Cancel mid-flight; nothing must leave the building."""
    executor = DispatchExecutor(
        session_factory, bank, clock=clock, hooks=PhaseHooks(on_in_flight=gate)
    )
    task = asyncio.create_task(executor.dispatch(state, state.next_candidate()))
    await gate.wait()

    assert executor.request_abort("recipient went offline mid-send") is True
    gate.open()
    await asyncio.wait([task])

    rows = await attempt_rows(session_factory, state.alert.alert_id)
    assert rows[0].state == "aborted"
    assert rows[0].committed_at is None
    assert "offline" in rows[0].outcome_reason

    assert bank.delivered == [], "an aborted dispatch delivered a message"
    assert PRIYA not in state.notified
    assert PRIYA in state.attempted, "but the attempt is on record forever"


async def test_abort_after_commit_is_refused(executor, state):
    """`request_abort` returning False IS the commit point, as an API.

    Once the row says committed the message exists and cannot be unsent. The
    only honest response to new information is a supplement — decision row R5.
    """
    await executor.dispatch(state, state.next_candidate())
    assert executor.current_attempt.state is AttemptState.COMMITTED
    assert executor.request_abort("too late") is False


async def test_shielded_commit_lands_even_when_cancelled_mid_write(
    session_factory, state, bank, clock
):
    """asyncio.shield() guards the commit write and nothing else.

    Cancellation arrives while the commit is in progress. The outer task dies;
    the write still lands. And crucially no `aborted` row is written over it —
    an audit trail that claims a delivered message was abandoned is worse than
    no audit trail.
    """
    cancelled_at_commit: dict = {}

    async def cancel_during_commit(attempt):
        task = cancelled_at_commit["task"]
        asyncio.get_running_loop().call_soon(task.cancel)

    executor = DispatchExecutor(
        session_factory,
        bank,
        clock=clock,
        hooks=PhaseHooks(before_commit=cancel_during_commit),
    )
    task = asyncio.create_task(executor.dispatch(state, state.next_candidate()))
    cancelled_at_commit["task"] = task
    await asyncio.wait([task])

    rows = await attempt_rows(session_factory, state.alert.alert_id)
    assert rows[0].state == "committed", "the shield did not protect the commit"
    assert rows[0].committed_at is not None


# ─────────────────────────────────────────────────────────────────────────────
# 6, 7, 8 — invariant I2, failure taxonomy, cancellation semantics
# ─────────────────────────────────────────────────────────────────────────────


async def test_aborted_attempt_blocks_reattempt_of_same_person(
    session_factory, state, bank, gate, clock
):
    """INVARIANT I2, enforced by the database rather than by memory.

    The idempotency key was taken at reservation, so it survives the abort. A
    second attempt on the same person for the same alert is a write SQLite
    refuses — not a rule the code has to remember.
    """
    executor = DispatchExecutor(
        session_factory, bank, clock=clock, hooks=PhaseHooks(on_in_flight=gate)
    )
    candidate = state.next_candidate()
    task = asyncio.create_task(executor.dispatch(state, candidate))
    await gate.wait()
    executor.request_abort("aborted for test")
    gate.open()
    await asyncio.wait([task])

    fresh = DispatchExecutor(session_factory, bank, clock=clock)
    with pytest.raises(DuplicateDispatchError) as exc_info:
        await fresh.dispatch(state, candidate, role="reroute")
    assert "idempotency" in str(exc_info.value).lower() or PRIYA in str(exc_info.value)

    async with session_factory() as session:
        total = await session.scalar(
            select(func.count()).select_from(DispatchAttempt)
        )
    assert total == 1, "a duplicate attempt row was created"


async def test_connect_refusal_fails_over_to_the_next_healthy_channel(
    executor, state, bank, session_factory
):
    """ROW R6, AT THE TRANSPORT LEVEL.

    A refused handshake is a transport fault, not a reason to change person —
    that would be an over-reaction to a broken pipe. So the executor walks this
    person's remaining healthy channels on the SAME attempt row, keeping the
    same idempotency key.

    Updated in Module 4: before the failover path existed, a connect refusal was
    terminal and this test expected ChannelConnectError. The new behaviour is
    strictly better, and the two assertions that matter are that Priya still has
    exactly ONE row and that the key never changed.
    """
    bank.fail(Channel.SLACK, on_connect=True)   # Priya prefers slack

    attempt = await executor.dispatch(state, state.next_candidate())

    assert attempt.state is AttemptState.COMMITTED
    assert attempt.channel is Channel.SMS, "should have moved to the next pipe"

    rows = await attempt_rows(session_factory, state.alert.alert_id)
    assert len(rows) == 1, "a failover must not create a second attempt row"
    assert rows[0].channel == "sms"
    assert rows[0].idempotency_key == f"{state.alert.alert_id}:{PRIYA}"

    delivered = [(channel.value, recipient) for channel, recipient, _ in bank.delivered]
    assert delivered == [("sms", PRIYA)], "one notification, down a different pipe"


async def test_every_channel_refusing_is_failed_not_aborted(
    executor, state, bank, session_factory
):
    """Only when EVERY transport refuses is the person genuinely unreachable.

    `failed` and `aborted` are different stories: the transport turned us away
    versus we changed our mind. Both are terminal and both keep the person in
    `attempted`, but conflating them makes the audit trail lie.
    """
    for channel in (Channel.SLACK, Channel.SMS, Channel.EMAIL):
        bank.fail(channel, on_connect=True)

    with pytest.raises(ChannelConnectError):
        await executor.dispatch(state, state.next_candidate())

    rows = await attempt_rows(session_factory, state.alert.alert_id)
    assert rows[0].state == "failed"
    assert rows[0].state != "aborted"
    assert rows[0].committed_at is None
    assert PRIYA in state.attempted
    assert bank.delivered == [], "nothing was transmitted on any pipe"


async def test_cancelled_error_is_reraised_when_not_deliberate(
    session_factory, state, bank, gate, clock
):
    """A cancellation we did not request must propagate.

    Swallowing one breaks TaskGroup shutdown and can hang the process — the
    failure mode is a suite that stops producing output rather than one that
    fails. `abort_requested` is what tells our decision apart from a shutdown.
    """
    executor = DispatchExecutor(
        session_factory, bank, clock=clock, hooks=PhaseHooks(on_in_flight=gate)
    )
    task = asyncio.create_task(executor.dispatch(state, state.next_candidate()))
    await gate.wait()

    task.cancel()  # NOT via request_abort — this is an external shutdown
    gate.open()
    await asyncio.wait([task])

    assert task.cancelled(), "an unrequested cancellation was swallowed"
    assert executor.current_attempt.abort_requested is False

    rows = await attempt_rows(session_factory, state.alert.alert_id)
    assert rows[0].state == "aborted", "the row is still written before re-raising"


async def test_illegal_transition_is_refused(executor, state):
    """§3.6 is enforced on every write, so an impossible history cannot be
    recorded. The audit trail is evidence; evidence that permits nonsense is
    not evidence."""
    attempt = await executor.dispatch(state, state.next_candidate())
    assert attempt.state is AttemptState.COMMITTED

    with pytest.raises(IllegalTransition):
        await executor._transition(attempt, AttemptState.CONNECTING)


# ─────────────────────────────────────────────────────────────────────────────
# 9, 10 — the interrupt path
# ─────────────────────────────────────────────────────────────────────────────


async def test_listener_drops_events_for_unevaluated_people(state, bus):
    """Row R1, arriving early. Cross-domain traffic names strangers constantly;
    dropping them before any work is the cheapest possible filter."""
    subscription = bus.subscribe()
    listener = InterruptListener(state, subscription)

    await listener.handle(
        InterruptEvent(
            kind=InterruptKind.PRESENCE_CHANGED,
            stakeholder_id=SOFIA,
            previous=Availability.BUSY,
            current=Availability.OFFLINE,
            at=1.0,
        )
    )
    assert listener.ignored == 1
    assert listener.applied == 0
    assert SOFIA not in state.observed
    assert state.audit == [], "an irrelevant event should not enter the record"

    subscription.unsubscribe()


async def test_better_match_derived_without_any_new_pull(
    state, bus, session_factory, critical_alert
):
    """The `escalate` shape: Elena comes online and out-qualifies Priya.

    The derivation reads ONLY state.observed — facts already bought by the one
    pull. Assert the evaluations ledger does not move, because that is the
    claim the brief cares most about.
    """
    state.register_attempt(_reserve_stub(state, PRIYA))
    subscription = bus.subscribe()
    listener = InterruptListener(state, subscription)

    async with session_factory() as session:
        before = await session.scalar(
            select(func.count()).select_from(Evaluation).where(
                Evaluation.alert_id == critical_alert.alert_id
            )
        )

    await listener.handle(
        InterruptEvent(
            kind=InterruptKind.PRESENCE_CHANGED,
            stakeholder_id=ELENA,
            previous=Availability.OFFLINE,
            current=Availability.ONLINE,
            at=2.0,
        )
    )

    derived = listener._derived
    assert ELENA in derived, "Elena at 140 must out-qualify Priya at 139"

    async with session_factory() as session:
        after = await session.scalar(
            select(func.count()).select_from(Evaluation).where(
                Evaluation.alert_id == critical_alert.alert_id
            )
        )
    assert after == before == 7, "deriving a better match spent query budget"

    subscription.unsubscribe()


def _reserve_stub(state, stakeholder_id: str):
    """An in-memory attempt, so the listener has an incumbent to compare against
    without running a real dispatch."""
    from alert_router.schemas import AttemptRecord

    return AttemptRecord(
        alert_id=state.alert.alert_id,
        stakeholder_id=stakeholder_id,
        channel=Channel.SLACK,
        role="primary",
        state=AttemptState.IN_FLIGHT,
        reserved_at=1.0,
    )


# ─────────────────────────────────────────────────────────────────────────────
# 11 — end to end: abort and re-route, with the query counter frozen
# ─────────────────────────────────────────────────────────────────────────────


async def test_agent_aborts_and_reroutes_without_new_queries(
    registry, session_factory, bank, clock, critical_alert, gate
):
    """Appendix A's `reroute` scenario, end to end.

    Priya drops offline while her Slack message is in flight. The dispatch is
    aborted inside the window, the ladder is walked, Tom is notified — and the
    evaluations ledger never moves, because everything the agent learned came
    from the event payload.
    """

    # The handler signals once the abort has actually been requested. Without
    # this the test races: set_status() only PUBLISHES the event, and opening
    # the gate immediately afterwards can let the dispatch run to completion
    # before the listener has even dequeued it. Waiting on a signal makes
    # "the interrupt landed pre-commit" a fact rather than a hope.
    abort_requested = asyncio.Event()

    async def abort_on_presence_drop(state, event, attempt):
        if event.kind is InterruptKind.PRESENCE_CHANGED and event.went_offline:
            agent.abort_current(f"{event.stakeholder_id} went offline mid-send")
            abort_requested.set()

    agent = AlertAgent(
        registry,
        session_factory,
        bank,
        clock=clock,
        hooks=PhaseHooks(on_in_flight=gate),
        on_interrupt=abort_on_presence_drop,
    )

    task = asyncio.create_task(agent.handle(critical_alert))
    await gate.wait()                       # parked mid-send to Priya
    await registry.set_status(PRIYA, Availability.OFFLINE, reason="laptop closed")
    await asyncio.wait_for(abort_requested.wait(), TIMEOUT)
    gate.open()                             # only now let the dispatches proceed

    state = await asyncio.wait_for(task, TIMEOUT)

    assert state.attempted[PRIYA].state is AttemptState.ABORTED
    assert TOM in state.notified
    assert PRIYA not in state.notified
    assert len(state.notified) == 1, "exactly one person was notified"

    delivered = [recipient for _, recipient, _ in bank.delivered]
    assert delivered == [TOM], f"expected only Tom to receive a message, got {delivered}"

    assert await registry.evaluation_count(critical_alert.alert_id) == 7
    assert len(state.evaluated) == 7

    rows = await attempt_rows(session_factory, critical_alert.alert_id)
    assert [(r.stakeholder_id, r.state) for r in rows] == [
        (PRIYA, "aborted"),
        (TOM, "committed"),
    ]
    assert [r.role for r in rows] == ["primary", "reroute"]


async def test_agent_records_a_readable_audit_trail(
    agent, critical_alert, session_factory
):
    """The trail must narrate the incident well enough for someone who has never
    seen the code to follow it. That is the standard, because it is what goes on
    screen in the walkthrough."""
    state = await asyncio.wait_for(agent.handle(critical_alert), TIMEOUT)

    kinds = [event.kind for event in state.audit]
    assert kinds[0] == "RESOLVED"
    assert "RANKED" in kinds
    assert "RESERVED" in kinds
    assert "COMMITTED" in kinds
    assert [e.seq for e in state.audit] == list(range(len(state.audit)))
    assert any("one query" in e.summary for e in state.audit)
