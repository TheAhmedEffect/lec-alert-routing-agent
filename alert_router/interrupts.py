"""
The interrupt listener — how the agent learns things without asking.

THE CLAIM THIS FILE HAS TO SUPPORT
----------------------------------
    "detect mid-execution that availability has changed, WITHOUT re-querying
     stakeholders you have already evaluated"

Nothing in this module may pull. There is deliberately no Registry import and no
call to query_by_domain or fetch_one — the listener works entirely from the
event payload and from observations already paid for. You can verify that with
one grep, which is the point.

WHAT ARRIVES vs WHAT IS DERIVED
-------------------------------
PRESENCE_CHANGED and CHANNEL_DEGRADED arrive on the bus, published by whoever
mutated the registry. BETTER_MATCH never does: it is DERIVED here, by re-scoring
patched observations and noticing that a ladder member now out-qualifies the
incumbent. That derivation is free — every fact it uses was bought by the one
pull — which is exactly why "a more senior person became available" can be
handled without spending query budget.
"""

from __future__ import annotations

import time
from typing import Awaitable, Callable

from .registry import Subscription
from .schemas import AttemptRecord, InterruptEvent, InterruptKind
from .state import DispatchState

Clock = Callable[[], float]

#: Module 4 supplies the decision matrix here. Module 3 defaults to a no-op that
#: only records the event, so the executor and the listener can be tested
#: independently of routing policy.
InterruptHandler = Callable[
    [DispatchState, InterruptEvent, AttemptRecord | None], Awaitable[None]
]


async def _noop_handler(
    state: DispatchState, event: InterruptEvent, attempt: AttemptRecord | None
) -> None:
    return None


class InterruptListener:
    """Consumes presence events for ONE alert and keeps its state current."""

    def __init__(
        self,
        state: DispatchState,
        subscription: Subscription,
        *,
        handler: InterruptHandler | None = None,
        clock: Clock = time.time,
    ) -> None:
        self.state = state
        self.subscription = subscription
        self.handler = handler or _noop_handler
        self._clock = clock

        #: Events that named somebody this alert never evaluated. Counted rather
        #: than logged because on a busy bus this is most of the traffic, and
        #: the count is the interesting number.
        self.ignored = 0
        self.applied = 0
        #: BETTER_MATCH is emitted at most once per person, so a flurry of
        #: presence events cannot produce a storm of identical escalations.
        self._derived: set[str] = set()

    # ── relevance ───────────────────────────────────────────────────────────

    def is_relevant(self, event: InterruptEvent) -> bool:
        """Is this event about somebody we are actually routing?

        Cross-domain presence traffic constantly names strangers. Dropping them
        here — before any patching, re-scoring or decision work — is decision
        matrix row R1, and it is the cheapest possible place to do it.
        """
        if self.state.in_ladder(event.stakeholder_id):
            return True
        attempt = self.state.current_attempt
        return attempt is not None and attempt.stakeholder_id == event.stakeholder_id

    # ── derivation ──────────────────────────────────────────────────────────

    def derive_better_match(self) -> InterruptEvent | None:
        """Has a patched observation made somebody out-qualify the incumbent?

        Constraints this respects, all of them load-bearing:

          * only LADDER MEMBERS are considered, so the target always has a
            paid-for observation and invariant I3 holds;
          * `qualification` alone decides, never reachability, so this cannot
            promote someone merely because they are at their desk (I4);
          * the candidate must be reachable, since paging an unreachable person
            in parallel achieves nothing;
          * anyone already attempted or suppressed is skipped — I2 outranks
            optimality, and that is row R8.
        """
        incumbent = self.state.incumbent
        if incumbent is None:
            return None

        for candidate in self.state.plan.ladder:
            person_id = candidate.snapshot.stakeholder.id
            if person_id == incumbent.snapshot.stakeholder.id:
                continue
            if person_id in self._derived:
                continue
            if person_id in self.state.attempted or person_id in self.state.suppressed:
                continue

            observed = self.state.observed.get(person_id)
            if observed is None or observed.status.reachability == 0:
                continue
            if candidate.score.qualification <= incumbent.score.qualification:
                continue

            self._derived.add(person_id)
            return InterruptEvent(
                kind=InterruptKind.BETTER_MATCH,
                stakeholder_id=person_id,
                current=observed.status,
                at=self._clock(),
                reason=(
                    f"{candidate.snapshot.stakeholder.name} "
                    f"({candidate.score.qualification:g}) now reachable and "
                    f"out-qualifies {incumbent.snapshot.stakeholder.name} "
                    f"({incumbent.score.qualification:g})"
                ),
            )
        return None

    # ── the loop ────────────────────────────────────────────────────────────

    async def handle(self, event: InterruptEvent) -> None:
        """Apply one event: filter, patch, re-score, derive, decide."""
        if not self.is_relevant(event):
            self.ignored += 1
            return

        if event.kind is InterruptKind.PRESENCE_CHANGED:
            # THE ENTIRE MECHANISM, IN TWO CALLS. patch_from_push copies the new
            # state out of the event payload; rescore recomputes from
            # observations already bought. No question is asked, so the
            # evaluations ledger does not move.
            if not self.state.patch_from_push(event):
                self.ignored += 1
                return
            self.state.rescore()

        self.applied += 1
        await self.state.record_audit(
            "INTERRUPT",
            event.stakeholder_id,
            _describe(event),
            {"kind": event.kind.value, "learned_via": "push"},
        )

        await self.handler(self.state, event, self.state.current_attempt)

        # A presence change may have promoted somebody. Deriving it costs
        # nothing and must never be confused with discovering somebody new.
        if event.kind is InterruptKind.PRESENCE_CHANGED:
            derived = self.derive_better_match()
            if derived is not None:
                await self.state.record_audit(
                    "INTERRUPT", derived.stakeholder_id, derived.reason,
                    {"kind": derived.kind.value, "learned_via": "derived"},
                )
                await self.handler(self.state, derived, self.state.current_attempt)

    async def run(self) -> None:
        """Consume until the subscription is stopped or the bus closes.

        Terminates on the sentinel rather than on cancellation, so a normal
        shutdown is distinguishable from a failure inside a TaskGroup.
        """
        async for event in self.subscription:
            await self.handle(event)


def _describe(event: InterruptEvent) -> str:
    if event.kind is InterruptKind.PRESENCE_CHANGED:
        previous = event.previous.value if event.previous else "?"
        current = event.current.value if event.current else "?"
        suffix = f" ({event.reason})" if event.reason else ""
        return f"presence {previous} -> {current}{suffix}"
    if event.kind is InterruptKind.CHANNEL_DEGRADED:
        channel = event.channel.value if event.channel else "?"
        health = "healthy" if event.healthy else "degraded"
        return f"channel {channel} {health}: {event.reason}"
    return event.reason or event.kind.value
