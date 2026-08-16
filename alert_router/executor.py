"""
The dispatch executor — the two-phase send and the commit point.

THIS FILE IS THE ASSESSMENT
---------------------------
Everything in Modules 1 and 2 was scaffolding for one question: what happens to
a notification that is already in the air when the world changes underneath it?

    RESERVE ....... INSERT dispatch_attempts(state='reserved'), COMMIT IT
        |           the idempotency key is taken from HERE ON, which is why an
        |           aborted attempt still blocks a retry (invariant I2)
        v
    CONNECTING .... adapter handshake
        |           refused -> state='failed'  (NOT 'aborted' — see below)
        v
    IN_FLIGHT ..... THE ABORT WINDOW. Cancellation here is clean, because
        |           nothing has left the building.
        v
    === asyncio.shield() guards the next write ===
    COMMITTED ..... state='committed' AND committed_at, in one UPDATE
                    a one-way door: supplements only, never retractions

TWO THINGS TO GET RIGHT, AND WHY
--------------------------------
1. THE SEND IS NEVER SHIELDED. Shield it and the dispatch becomes uncancellable,
   the abort window disappears, and the demo has nothing to show.

   There are exactly two shields in this file, and neither protects the send:

     * around the COMMIT write, so a delivered message is always recorded as
       delivered even if the task is being torn down;
     * around the ABORT write, because that cleanup runs inside an
       already-cancelled task and an unshielded await there would itself be
       cancelled — leaving the row stuck at `in_flight`, the session half-open,
       and the audit trail silent about an abort that definitely happened.

   One makes success durable, the other makes failure durable. Neither makes the
   send uninterruptible, which is the whole point.

2. `CancelledError` is caught so the `aborted` row can be written, and then
   RE-RAISED unless we asked for the cancellation ourselves. Swallowing a
   cancellation we did not request breaks TaskGroup shutdown and can hang the
   process. `AttemptRecord.abort_requested` is how the two are told apart.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Awaitable, Callable

from sqlalchemy import update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .channels import (
    ChannelBank,
    ChannelConnectError,
    ChannelSendError,
    first_healthy_channel,
    healthy_channels,
)
from .context import deliver_envelope
from .models_orm import DispatchAttempt
from .schemas import AttemptRecord, AttemptState, Channel, RankedCandidate
from .state import DispatchState

Clock = Callable[[], float]
SessionFactory = async_sessionmaker[AsyncSession]
Hook = Callable[[AttemptRecord], Awaitable[None]]


class DuplicateDispatchError(RuntimeError):
    """INVARIANT I2, as raised by the database.

    The UNIQUE on dispatch_attempts.idempotency_key refused a second attempt for
    this (alert, person). Named separately from every other IntegrityError for
    the same reason DuplicateQueryError is: a blanket catch would disguise a
    foreign-key or CHECK failure as a duplicate, and send you debugging the
    wrong invariant.
    """


#: §3.6, expressed as data. Enforced on every write so an impossible history
#: cannot be recorded — the audit trail is evidence, and evidence that permits
#: nonsense is not evidence.
LEGAL_TRANSITIONS: dict[AttemptState, frozenset[AttemptState]] = {
    AttemptState.RESERVED: frozenset({AttemptState.CONNECTING, AttemptState.ABORTED}),
    AttemptState.CONNECTING: frozenset(
        {AttemptState.IN_FLIGHT, AttemptState.ABORTED, AttemptState.FAILED}
    ),
    AttemptState.IN_FLIGHT: frozenset(
        {AttemptState.COMMITTED, AttemptState.ABORTED, AttemptState.FAILED}
    ),
    AttemptState.COMMITTED: frozenset(),
    AttemptState.ABORTED: frozenset(),
    AttemptState.FAILED: frozenset(),
}


#: §3.6 has one documented exception, and this is it. A CHANNEL FAILOVER re-opens
#: the handshake on a different pipe for the SAME notification — same row, same
#: idempotency key — so the attempt legitimately moves backwards into
#: `connecting`. It is not a new attempt; it is the same one, still trying.
#: Allowed only through failover(), never through _transition().
FAILOVER_ENTRY_STATES = frozenset(
    {AttemptState.CONNECTING, AttemptState.IN_FLIGHT, AttemptState.ABORTED}
)


class IllegalTransition(RuntimeError):
    """An attempt tried to move somewhere §3.6 does not allow."""


@dataclass
class PhaseHooks:
    """Awaitables fired at known points in the send.

    THIS IS WHY THE TESTS ARE NOT FLAKY. "Inject the interrupt at t+0.8s" is
    fine for a demo and fatal for a suite: it passes on your laptop and fails
    once in twenty runs, on camera, with no way to explain it. Tests instead
    park the executor on an Event at a precise phase, land the interrupt, and
    release — so pre-commit and post-commit are chosen, not raced for.
    """

    on_reserved: Hook | None = None
    on_connecting: Hook | None = None
    on_in_flight: Hook | None = None
    before_commit: Hook | None = None
    after_commit: Hook | None = None


class DispatchExecutor:
    """Runs one attempt at a time for one alert.

    Takes a session FACTORY, never a session: this executor and the interrupt
    listener run as concurrent tasks, and an AsyncSession is not
    concurrency-safe. Sharing one produces interleaved transactions and sporadic
    InvalidRequestError — symptoms surreal enough to waste a day on.
    """

    def __init__(
        self,
        session_factory: SessionFactory,
        bank: ChannelBank,
        *,
        clock: Clock = time.time,
        hooks: PhaseHooks | None = None,
    ) -> None:
        self._sessions = session_factory
        self.bank = bank
        self._clock = clock
        self.hooks = hooks or PhaseHooks()
        self._task: asyncio.Task | None = None
        self._attempt: AttemptRecord | None = None
        #: Set by the decision matrix (row R6 / R4) when the in-flight attempt
        #: should move to a different pipe rather than to a different person.
        #: The ladder walk reads it after the abort and calls failover().
        self.pending_failover: Channel | None = None
        #: Non-None only between "commit started" and "dispatch returned". The
        #: cancellation handler reads it to avoid overwriting a landed commit.
        self._commit_task: asyncio.Task | None = None

    # ── abort control ───────────────────────────────────────────────────────

    def request_abort(self, reason: str) -> bool:
        """Deliberately cancel the in-flight attempt. False if it is too late.

        Returning False IS the commit point, expressed as an API. Once the row
        says `committed` the message exists in the world and cannot be unsent,
        so the only honest response to new information is a supplement — which
        is decision matrix row R5, not a retraction.

        Requires dispatch() to be running in its own task (the agent arranges
        that); cancelling otherwise would tear down the caller.
        """
        attempt = self._attempt
        if attempt is None or not attempt.state.is_pre_commit:
            return False
        if self._task is None or self._task.done():
            return False
        attempt.abort_requested = True
        attempt.outcome_reason = reason
        self._task.cancel()
        return True

    @property
    def current_attempt(self) -> AttemptRecord | None:
        return self._attempt

    # ── the send ────────────────────────────────────────────────────────────

    async def dispatch(
        self,
        state: DispatchState,
        candidate: RankedCandidate,
        *,
        role: str = "primary",
        channel: Channel | None = None,
        body: str = "",
    ) -> AttemptRecord:
        """Reserve, connect, send, commit. Cancellable until the shield."""
        self._task = asyncio.current_task()
        self._commit_task = None
        person = candidate.snapshot.stakeholder

        chosen = channel or await first_healthy_channel(self._sessions, person)
        if chosen is None:
            raise ChannelConnectError(f"{person.name} has no healthy channel")

        attempt = AttemptRecord(
            alert_id=state.alert.alert_id,
            stakeholder_id=person.id,
            channel=chosen,
            role=role,  # type: ignore[arg-type]
            state=AttemptState.RESERVED,
            plan_version=state.plan.plan_version,
            reserved_at=self._clock(),
        )
        self._attempt = attempt

        # ── RESERVE ─────────────────────────────────────────────────────────
        # Inserted AND COMMITTED before a single byte moves. The idempotency key
        # is claimed by the decision to try, not by the delivery — which is
        # precisely what makes an aborted attempt block a later retry.
        await self._reserve(attempt)
        state.register_attempt(attempt, make_current=(role != "escalation"))
        await state.record_audit(
            "RESERVED",
            person.id,
            f"reserved {role} dispatch to {person.name} on {chosen.value}",
            {"idempotency_key": attempt.idempotency_key},
        )
        await self._fire(self.hooks.on_reserved, attempt)

        adapter = self.bank[chosen]
        try:
            # ── CONNECTING ──────────────────────────────────────────────────
            await self._transition(attempt, AttemptState.CONNECTING)
            await self._fire(self.hooks.on_connecting, attempt)

            # DECISION ROW R6, IN THE CONNECT PATH. A refused handshake is a
            # transport fault, not a reason to change person — that would be an
            # over-reaction. So walk this person's remaining healthy pipes on
            # the SAME attempt row, keeping the same idempotency key. Only when
            # every transport refuses (row R7) does the person become genuinely
            # unreachable and the attempt fail.
            fallbacks = [
                candidate_channel
                for candidate_channel in await healthy_channels(
                    self._sessions, person
                )
                if candidate_channel != chosen
            ]
            for index, pipe in enumerate([chosen, *fallbacks]):
                if index > 0:
                    await self._switch_channel(state, attempt, pipe)
                    adapter = self.bank[pipe]
                try:
                    await adapter.connect(person.id)
                    break
                except ChannelConnectError as exc:
                    if index == len(fallbacks):
                        # The transport refused us on every pipe. That is
                        # `failed`, not `aborted` — we did not change our mind,
                        # we were turned away.
                        await self._transition(
                            attempt, AttemptState.FAILED, reason=str(exc)
                        )
                        await state.record_audit(
                            "ABORTED",
                            person.id,
                            f"every channel refused {person.name}: {exc}",
                        )
                        raise
            chosen = attempt.channel

            # ── IN_FLIGHT — the abort window ────────────────────────────────
            await self._transition(attempt, AttemptState.IN_FLIGHT)
            await self._fire(self.hooks.on_in_flight, attempt)
            receipt = await adapter.send(person.id, body or _default_body(state))

            # ── COMMIT ──────────────────────────────────────────────────────
            await self._fire(self.hooks.before_commit, attempt)
            # THE ONLY SHIELD IN THIS FILE. It guards the write that makes the
            # notification real, and nothing else. Everything above this line is
            # cancellable; nothing below it is.
            #
            # The inner coroutine is held as a task so the cancellation handler
            # can tell "cancelled before the commit began" from "cancelled while
            # the commit was landing". Without that distinction a late external
            # cancellation would write an `aborted` row over a `committed` one,
            # and the audit trail would claim a delivered message was abandoned.
            self._commit_task = asyncio.ensure_future(self._commit(attempt, receipt))
            await asyncio.shield(self._commit_task)
            state.mark_notified(person.id)
            await state.record_audit(
                "COMMITTED",
                person.id,
                f"delivered to {person.name} on {chosen.value} as {role}",
                {"receipt": receipt},
            )
            # Presentation, strictly AFTER the one-way door. Rendering must never
            # be able to delay or fail a notification that is already decided.
            await deliver_envelope(state, attempt)
            await self._fire(self.hooks.after_commit, attempt)
            return attempt

        except asyncio.CancelledError:
            if self._commit_task is not None:
                # The commit was already under way when the cancellation
                # arrived. The shield guarantees that write lands, so this
                # dispatch DID deliver — recording an abort here would make the
                # audit trail lie. Let the cancellation propagate untouched.
                raise

            # Write the aborted row, then decide whether this cancellation was
            # ours. If it was not, re-raise: swallowing a cancellation we did
            # not request breaks TaskGroup shutdown and can hang the process.
            #
            # THE SHIELD HERE IS NOT OPTIONAL. We are already inside a cancelled
            # task, and an unshielded await would be cancelled too — leaving the
            # row stuck at `in_flight` forever, the session half-closed, and the
            # audit trail silent about an abort that definitely happened.
            # Cleanup after cancellation has to be protected or it does not run.
            reason = attempt.outcome_reason or "cancelled"
            if attempt.state.is_pre_commit:
                await asyncio.shield(
                    self._record_abort(state, attempt, person, chosen, role, reason)
                )
            if not attempt.abort_requested:
                raise
            return attempt

        except ChannelSendError as exc:
            await self._transition(attempt, AttemptState.FAILED, reason=str(exc))
            await state.record_audit(
                "ABORTED", person.id, f"send failed on {chosen.value}: {exc}"
            )
            raise

    # ── persistence ─────────────────────────────────────────────────────────

    async def _reserve(self, attempt: AttemptRecord) -> None:
        statement = sqlite_insert(DispatchAttempt).values(
            alert_id=attempt.alert_id,
            stakeholder_id=attempt.stakeholder_id,
            idempotency_key=attempt.idempotency_key,
            channel=attempt.channel.value,
            role=attempt.role,
            state=AttemptState.RESERVED.value,
            plan_version=attempt.plan_version,
            reserved_at=attempt.reserved_at,
            committed_at=None,
            outcome_reason=None,
        )
        try:
            async with self._sessions() as session:
                await session.execute(statement)
                await session.commit()
        except IntegrityError as exc:
            message = str(getattr(exc, "orig", exc)).lower()
            if "unique constraint failed" in message and "idempotency_key" in message:
                raise DuplicateDispatchError(
                    f"{attempt.stakeholder_id} has already been attempted for "
                    f"alert {attempt.alert_id!r} — key {attempt.idempotency_key}"
                ) from exc
            raise  # foreign key, CHECK, anything else: not ours to relabel

    async def _transition(
        self,
        attempt: AttemptRecord,
        to_state: AttemptState,
        *,
        reason: str | None = None,
    ) -> None:
        """Move the attempt, validating against §3.6 and writing the row.

        NOTE `committed_at` IS NOT TOUCHED HERE. The schema carries
            CHECK ((state='committed' AND committed_at IS NOT NULL)
                OR (state<>'committed' AND committed_at IS NULL))
        so setting it on a non-committed row is refused by the database, and
        committing without it is too. _commit() sets both in one write; this
        method deliberately handles every state except COMMITTED.
        """
        if to_state is AttemptState.COMMITTED:
            raise IllegalTransition("use _commit() — committed_at must be set atomically")
        if to_state not in LEGAL_TRANSITIONS[attempt.state]:
            raise IllegalTransition(
                f"{attempt.state.value} -> {to_state.value} is not a legal transition"
            )

        attempt.state = to_state
        if reason:
            attempt.outcome_reason = reason

        values: dict = {"state": to_state.value}
        if reason:
            values["outcome_reason"] = reason
        async with self._sessions() as session:
            await session.execute(
                update(DispatchAttempt)
                .where(DispatchAttempt.idempotency_key == attempt.idempotency_key)
                .values(**values)
            )
            await session.commit()

    # ── channel failover — invariant I2's most interesting consequence ──────

    async def failover(
        self,
        state: DispatchState,
        attempt: AttemptRecord,
        new_channel: Channel,
    ) -> AttemptRecord:
        """Move an existing attempt to a different pipe. SAME ROW, SAME KEY.

        THIS IS NOT A SECOND DISPATCH, and it must not be one. The idempotency
        key is '{alert_id}:{stakeholder_id}' — the channel is deliberately
        excluded — so calling dispatch() again for the same person raises
        DuplicateDispatchError. That is the schema working exactly as designed:
        the same notification down a different pipe is ONE notification, and
        invariant I2 says one notification per person.

        So this UPDATEs `channel` on the row that already exists, re-opens the
        handshake, and finishes the send. The audit trail shows a failover, not
        a duplicate, because that is what actually happened.
        """
        if attempt.state not in FAILOVER_ENTRY_STATES:
            raise IllegalTransition(
                f"cannot fail over from {attempt.state.value} — the attempt is "
                "terminal or has already committed"
            )

        person = state.candidate_for(attempt.stakeholder_id).snapshot.stakeholder
        attempt.abort_requested = False
        self._attempt = attempt
        self._task = asyncio.current_task()
        self._commit_task = None
        self.pending_failover = None

        await self._switch_channel(state, attempt, new_channel)

        adapter = self.bank[new_channel]
        await adapter.connect(person.id)
        await self._transition(attempt, AttemptState.IN_FLIGHT)
        receipt = await adapter.send(person.id, _default_body(state))

        self._commit_task = asyncio.ensure_future(self._commit(attempt, receipt))
        await asyncio.shield(self._commit_task)
        state.mark_notified(person.id)
        await state.record_audit(
            "COMMITTED",
            person.id,
            f"delivered to {person.name} on {new_channel.value} after failover",
            {"receipt": receipt},
        )
        await deliver_envelope(state, attempt)
        return attempt

    async def _switch_channel(
        self,
        state: DispatchState,
        attempt: AttemptRecord,
        new_channel: Channel,
    ) -> None:
        """UPDATE the pipe on an existing attempt row. Never touches the key.

        Shared by failover() and by the connect-retry inside dispatch(), because
        both are the same act: this notification is still going to this person,
        just down a different wire.
        """
        person_name = attempt.stakeholder_id
        candidate = state.candidate_for(attempt.stakeholder_id)
        if candidate is not None:
            person_name = candidate.snapshot.stakeholder.name

        previous = attempt.channel
        attempt.channel = new_channel
        attempt.state = AttemptState.CONNECTING

        async with self._sessions() as session:
            await session.execute(
                update(DispatchAttempt)
                .where(DispatchAttempt.idempotency_key == attempt.idempotency_key)
                .values(
                    channel=new_channel.value,
                    state=AttemptState.CONNECTING.value,
                    outcome_reason=f"failover {previous.value} -> {new_channel.value}",
                )
            )
            await session.commit()

        await state.record_audit(
            "DECISION",
            attempt.stakeholder_id,
            f"channel failover for {person_name}: {previous.value} -> "
            f"{new_channel.value} (same idempotency key {attempt.idempotency_key})",
        )

    async def _record_abort(
        self,
        state: DispatchState,
        attempt: AttemptRecord,
        person,
        channel: Channel,
        role: str,
        reason: str,
    ) -> None:
        """Persist the abort. Always invoked under a shield — see the caller."""
        await self._transition(attempt, AttemptState.ABORTED, reason=reason)
        await state.record_audit(
            "ABORTED",
            person.id,
            f"aborted {role} dispatch to {person.name} on {channel.value}: {reason}",
        )

    async def _commit(self, attempt: AttemptRecord, receipt: str) -> None:
        """The one-way door.

        `state` and `committed_at` are set in a SINGLE UPDATE because the CHECK
        constraint pairs them. Two separate writes would fail on the first one,
        and the temptation would be to weaken the constraint — do not. It is
        what makes "did this actually land?" answerable from the data alone.
        """
        if AttemptState.COMMITTED not in LEGAL_TRANSITIONS[attempt.state]:
            raise IllegalTransition(
                f"{attempt.state.value} -> committed is not a legal transition"
            )
        committed_at = self._clock()
        async with self._sessions() as session:
            await session.execute(
                update(DispatchAttempt)
                .where(DispatchAttempt.idempotency_key == attempt.idempotency_key)
                .values(
                    state=AttemptState.COMMITTED.value,
                    committed_at=committed_at,
                    outcome_reason=receipt,
                )
            )
            await session.commit()
        attempt.state = AttemptState.COMMITTED
        attempt.committed_at = committed_at
        attempt.outcome_reason = receipt

    # ── helpers ─────────────────────────────────────────────────────────────

    @staticmethod
    async def _fire(hook: Hook | None, attempt: AttemptRecord) -> None:
        if hook is not None:
            await hook(attempt)


def _default_body(state: DispatchState) -> str:
    """Placeholder payload. Module 5 replaces this with a rendered envelope."""
    alert = state.alert
    return f"[{alert.severity.value.upper()}] {alert.describe()}"
