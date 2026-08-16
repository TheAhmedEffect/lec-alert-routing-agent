"""
Top-level orchestration: one alert, two concurrent tasks, one frozen ladder.

THE ORDERING THAT MATTERS MOST IN THIS FILE
-------------------------------------------
`bus.subscribe()` is called in the PARENT coroutine, before either task starts,
and the resulting Subscription is passed in. Subscription registers its queue
synchronously in __init__ (see registry.py), so subscribing here guarantees the
listener's queue exists before dispatch can publish anything. Doing it inside the
listener task body registers at an unpredictable moment, and an event published
in the gap is lost — the wrong decision row fires, roughly one run in twenty.

WHERE THE DECISION MATRIX PLUGS IN (Module 4)
---------------------------------------------
`decisions.decide()` is PURE: facts in, RoutingDecision out. Every effect lives
in `apply()` below. That split is what lets the eleven-row truth table be tested
in milliseconds without a database, and it keeps the one place that can change
the world small enough to read in a sitting.

The one impure input the matrix needs — which transports are currently healthy —
is resolved here, in `_channel_facts()`, and passed in.
"""

from __future__ import annotations

import asyncio
import time
from typing import Callable

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .channels import ChannelBank, ChannelError, first_persistent_channel, healthy_channels
from .decisions import ChannelFacts, NoMatchingRow, decide
from .executor import DispatchExecutor, DuplicateDispatchError, PhaseHooks
from .interrupts import InterruptHandler, InterruptListener
from .models_orm import Alert as AlertRow
from .ranking import best_by_qualification
from .registry import Registry
from .schemas import (
    AlertEvent,
    AttemptRecord,
    AttemptState,
    DecisionAction,
    InterruptEvent,
    RankedCandidate,
    RoutingDecision,
)
from .state import DispatchState, persist_ladder

Clock = Callable[[], float]
SessionFactory = async_sessionmaker[AsyncSession]


class AlertAgent:
    """Routes one alert to one person, surviving the world changing mid-flight."""

    def __init__(
        self,
        registry: Registry,
        session_factory: SessionFactory,
        bank: ChannelBank,
        *,
        clock: Clock = time.time,
        hooks: PhaseHooks | None = None,
        on_interrupt: InterruptHandler | None = None,
    ) -> None:
        self.registry = registry
        self._sessions = session_factory
        self.bank = bank
        self._clock = clock
        self._hooks = hooks
        self.executor = DispatchExecutor(
            session_factory, bank, clock=clock, hooks=hooks
        )
        #: Defaults to the decision matrix. Tests may inject their own handler to
        #: drive the executor directly without routing policy in the way.
        self.on_interrupt = on_interrupt or self._decide_and_apply
        self.listener: InterruptListener | None = None
        #: Parallel escalations (rows R4, R5, R9). Tracked so handle() can await
        #: them — a dropped task would mean a page nobody sent and nobody missed.
        self._side_tasks: list[asyncio.Task] = []
        self.decisions: list[RoutingDecision] = []

    # ── entry point ─────────────────────────────────────────────────────────

    async def handle(self, alert: AlertEvent) -> DispatchState:
        snapshots = await self.registry.query_by_domain(alert)   # the ONE pull
        state = DispatchState.start(
            alert, snapshots, session_factory=self._sessions, clock=self._clock
        )
        await persist_ladder(self._sessions, state.plan)

        await state.record_audit(
            "RESOLVED",
            "registry",
            f"{len(snapshots)} candidates from one query on domain '{alert.domain}'",
            {"evaluated": sorted(state.evaluated)},
        )
        leader = state.plan.ladder[0] if state.plan.ladder else None
        await state.record_audit(
            "RANKED",
            "ranking",
            (
                f"{leader.snapshot.stakeholder.name} leads at "
                f"{leader.score.qualification:g} (reachability "
                f"{leader.score.reachability})"
                if leader
                else "no eligible candidates"
            ),
            {
                "ladder": [
                    {
                        "id": c.snapshot.stakeholder.id,
                        "qualification": c.score.qualification,
                        "reachability": c.score.reachability,
                    }
                    for c in state.plan.ladder
                ]
            },
        )

        # SUBSCRIBE IN THE PARENT — see the module docstring.
        subscription = self.registry.bus.subscribe()
        self.listener = InterruptListener(
            state, subscription, handler=self.on_interrupt, clock=self._clock
        )

        try:
            async with asyncio.TaskGroup() as group:
                group.create_task(self.listener.run(), name="interrupt-listener")
                group.create_task(self._walk_ladder(state), name="dispatch-loop")
        except* ChannelError as group_error:
            for error in group_error.exceptions:
                await state.record_audit("ABORTED", "channels", str(error))
        finally:
            subscription.unsubscribe()

        if self._side_tasks:
            await asyncio.wait(self._side_tasks)
            self._side_tasks.clear()

        await self._finalise(state)
        return state

    # ── the decision path ───────────────────────────────────────────────────

    async def _channel_facts(self, state: DispatchState, attempt) -> ChannelFacts:
        """Resolve the one impure input the matrix needs.

        decisions.py may not touch the database, so transport health is looked up
        here and handed over as plain data. Rows R6 and R7 turn on it.
        """
        if attempt is None:
            return ChannelFacts()
        candidate = state.candidate_for(attempt.stakeholder_id)
        if candidate is None:
            return ChannelFacts()
        person = candidate.snapshot.stakeholder
        return ChannelFacts(
            healthy=await healthy_channels(self._sessions, person),
            persistent=await first_persistent_channel(self._sessions, person),
        )

    async def _decide_and_apply(
        self, state: DispatchState, event: InterruptEvent, attempt
    ) -> RoutingDecision:
        channels = await self._channel_facts(state, attempt)
        try:
            decision = decide(state, event, attempt, channels)
        except NoMatchingRow as exc:
            # Loud on purpose. A gap in the matrix must not degrade into
            # "do nothing forever" — see NoMatchingRow's docstring.
            await state.record_audit("DECISION", "matrix", f"NO ROW MATCHED: {exc}")
            raise
        self.decisions.append(decision)
        await self.apply(state, decision, attempt)
        return decision

    async def apply(
        self,
        state: DispatchState,
        decision: RoutingDecision,
        attempt: AttemptRecord | None,
    ) -> None:
        """Every side effect the matrix implies. The ONLY impure half."""
        actor = attempt.stakeholder_id if attempt is not None else "matrix"
        await state.record_audit(
            "DECISION",
            actor,
            f"[{decision.matrix_row}] {decision.rationale}",
            {"action": decision.action.value, "matrix_row": decision.matrix_row},
        )

        # Suppression is recorded for EVERY decision that carries it, not just
        # R4. The numeric reason is the argument; dropping it turns a defensible
        # refusal into an unexplained skip.
        for person_id, why in decision.suppressed:
            if person_id not in state.suppressed:
                state.mark_suppressed(person_id, why)
                await state.record_audit("SUPPRESSED", person_id, why)

        action = decision.action

        if action is DecisionAction.CONTINUE_UNCHANGED:
            return

        if action is DecisionAction.EXHAUSTED:
            state.terminal = True
            return

        if action is DecisionAction.CHANNEL_FAILOVER:
            # Same person, same idempotency key, different pipe. The ladder walk
            # performs it once the in-flight send has stopped — see _walk_ladder.
            if decision.target_channel is not None:
                self.executor.pending_failover = decision.target_channel
                self.executor.request_abort(
                    f"failing over to {decision.target_channel.value}"
                )
            return

        if action is DecisionAction.ABORT_AND_REROUTE:
            aborted = self.executor.request_abort(decision.rationale)
            if not aborted:
                # THE COMMIT POINT MOVED UNDER US. The message has already
                # landed, so R3 is no longer available and pretending otherwise
                # would notify a second person about a delivered alert. The
                # honest correction is R5: supplement, never retract.
                fallback = best_by_qualification(state.remaining)
                await state.record_audit(
                    "DECISION",
                    actor,
                    f"[{decision.matrix_row}->R5] abort refused — the commit "
                    f"point had already passed; supplementing with a parallel "
                    f"escalation instead of re-routing",
                    {"action": "complete_and_escalate_parallel", "matrix_row": "R5"},
                )
                if fallback is not None:
                    self._spawn_escalation(state, fallback)
            return

        if action is DecisionAction.HOLD_AND_ESCALATE_UP:
            # Obligation 1 — keep the incumbent, on a channel that survives them
            # being away from their desk.
            if decision.target_channel is not None:
                self.executor.pending_failover = decision.target_channel
                self.executor.request_abort(
                    "holding the incumbent on a persistent channel"
                )
            # Obligation 2 — page the most qualified member, in parallel.
            # (Obligation 3, the numeric suppression, was recorded above.)
            self._spawn_escalation_by_id(state, decision.escalate_to_id)
            return

        if action is DecisionAction.COMPLETE_AND_ESCALATE_PARALLEL:
            self._spawn_escalation_by_id(state, decision.escalate_to_id)
            return

    # ── parallel escalation ─────────────────────────────────────────────────

    def _spawn_escalation_by_id(
        self, state: DispatchState, target_id: str | None
    ) -> None:
        if target_id is None:
            return
        candidate = state.candidate_for(target_id)
        if candidate is None:
            # Guardrail: escalating to a non-member would mean learning about
            # somebody we never paid to evaluate, which breaks invariant I3.
            return
        self._spawn_escalation(state, candidate)

    def _spawn_escalation(
        self, state: DispatchState, candidate: RankedCandidate
    ) -> None:
        person_id = candidate.snapshot.stakeholder.id
        if person_id in state.attempted or person_id in state.notified:
            return  # I2: never notify the same person twice
        task = asyncio.create_task(
            self._escalate(state, candidate), name=f"escalate-{person_id}"
        )
        self._side_tasks.append(task)

    async def _escalate(
        self, state: DispatchState, candidate: RankedCandidate
    ) -> None:
        """Page a second person ALONGSIDE the first.

        Uses its OWN executor. Sharing the primary one would overwrite
        `current_attempt`, and the matrix keys R2-versus-R5 off the incumbent's
        phase — so a parallel escalation would silently change which row fires
        for the next event.

        Different person means a different idempotency key, so two notifications
        here are two notifications, not a duplicate.
        """
        executor = DispatchExecutor(self._sessions, self.bank, clock=self._clock)
        try:
            await executor.dispatch(state, candidate, role="escalation")
        except (ChannelError, DuplicateDispatchError) as exc:
            await state.record_audit(
                "ABORTED",
                candidate.snapshot.stakeholder.id,
                f"parallel escalation failed: {exc}",
            )

    # ── the ladder walk ─────────────────────────────────────────────────────

    async def _walk_ladder(self, state: DispatchState) -> None:
        role = "primary"
        try:
            while not state.terminal:
                candidate = state.next_candidate()
                if candidate is None:
                    state.terminal = True
                    await state.record_audit(
                        "EXHAUSTED", "agent", "ladder exhausted with nothing committed"
                    )
                    return

                task = asyncio.create_task(
                    self.executor.dispatch(state, candidate, role=role),
                    name=f"dispatch-{candidate.snapshot.stakeholder.id}",
                )
                # asyncio.wait rather than `await task`: awaiting a cancelled
                # task raises CancelledError in THIS coroutine, which would tear
                # down the ladder walk we are trying to continue.
                await asyncio.wait({task})

                attempt = self.executor.current_attempt
                deliberate = attempt is not None and attempt.abort_requested

                if task.cancelled() and not deliberate:
                    raise asyncio.CancelledError

                error = None if task.cancelled() else task.exception()
                if error is not None and not isinstance(error, ChannelError):
                    raise error

                # ROW R6 / R4: change the pipe, not the person. Done here rather
                # than inside apply() because the send had to stop first — two
                # concurrent sends on one attempt would be a genuine duplicate.
                pipe = self.executor.pending_failover
                if pipe is not None and attempt is not None:
                    self.executor.pending_failover = None
                    try:
                        await self.executor.failover(state, attempt, pipe)
                        state.terminal = True
                        return
                    except ChannelError as exc:
                        await state.record_audit(
                            "ABORTED",
                            attempt.stakeholder_id,
                            f"failover to {pipe.value} failed: {exc}",
                        )
                        role = "reroute"
                        continue

                if error is not None:
                    role = "reroute"
                    continue

                # DO NOT TRUST task.cancelled() TO MEAN "aborted". dispatch()
                # catches a deliberate cancellation and returns the attempt
                # normally, so the task completes successfully even though
                # nothing was delivered. The ATTEMPT is the source of truth.
                if attempt is None or attempt.state is not AttemptState.COMMITTED:
                    role = "reroute"
                    continue

                state.terminal = True
                return
        finally:
            if self.listener is not None:
                self.listener.subscription.stop()

    # ── bookkeeping ─────────────────────────────────────────────────────────

    async def _finalise(self, state: DispatchState) -> None:
        """Record the alert's terminal state. Exhaustion must be LOUD."""
        if state.notified:
            final = "escalated" if len(state.notified) > 1 else "delivered"
        else:
            final = "exhausted"

        async with self._sessions() as session:
            await session.execute(
                update(AlertRow)
                .where(AlertRow.alert_id == state.alert.alert_id)
                .values(state=final)
            )
            await session.commit()
        state.terminal = True

    # ── helpers for scenarios and tests ─────────────────────────────────────

    def abort_current(self, reason: str) -> bool:
        """Ask the executor to cancel the in-flight attempt.

        Returns False once the commit point has passed — that boolean IS the
        abort window, exposed as an API, and it is what row R5 keys off.
        """
        return self.executor.request_abort(reason)

    @property
    def committed(self) -> bool:
        attempt = self.executor.current_attempt
        return attempt is not None and attempt.state is AttemptState.COMMITTED
