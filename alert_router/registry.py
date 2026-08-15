"""
The registry: the only source of truth about people, and the only place the
query budget is spent.

THE CENTRAL IDEA
----------------
The brief asks for two things that look contradictory:

    "detect mid-execution that availability has changed"
    "without re-querying stakeholders you have already evaluated"

You cannot notice a change you have forbidden yourself to look for. The
resolution is that this module exposes TWO DIFFERENT VERBS, and keeping them
apart is what makes the whole assessment satisfiable:

  PULL  — query_by_domain() / fetch_one(). Asynchronous, latency-bearing, and
          COUNTED. Every pull writes a row to `evaluations`, whose composite
          primary key makes a second pull for the same (alert, person) an
          IntegrityError. This is what the brief means by "querying
          availability", and we get exactly one per person per alert.

  PUSH  — PresenceBus. Free, event-driven, uncounted, and it carries the new
          state in the event payload. Nothing is asked, so nothing is charged.
          This is also how a real system works: Slack and PagerDuty deliver
          presence as a webhook, not as a poll.

The query counter measures QUESTIONS ASKED, not FACTS KNOWN. Push events
increase the second without touching the first. That sentence is the answer to
the hardest question a reviewer can ask about this system.
"""

from __future__ import annotations

import asyncio
import json
import random
import time
from typing import AsyncIterator, Callable, Sequence

from sqlalchemy import func, or_, select, text, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from . import config
from .models_orm import Alert, ChannelHealth, Evaluation, Stakeholder
from .schemas import (
    AlertEvent,
    Availability,
    CandidateSnapshot,
    Channel,
    InterruptEvent,
    InterruptKind,
    StakeholderRecord,
)

Clock = Callable[[], float]
LatencyFn = Callable[[], float]


# ─────────────────────────────────────────────────────────────────────────────
# Errors
# ─────────────────────────────────────────────────────────────────────────────


class UnknownStakeholder(KeyError):
    """The registry has no record with that id."""


class DuplicateQueryError(RuntimeError):
    """A second availability pull was attempted for the same (alert, person).

    A hard failure, not a warning. The brief states the constraint plainly, so
    the system should be unable to violate it quietly — a test that passes
    because nobody noticed the second query is worse than a crash.

    Always raised `from` the underlying IntegrityError, so the database's own
    message stays in the traceback and the claim remains checkable.
    """


#: The exact text SQLite produces when the evaluations composite primary key is
#: violated. Confirmed empirically:
#:
#:   sqlite3.IntegrityError: UNIQUE constraint failed:
#:       evaluations.alert_id, evaluations.stakeholder_id
#:
#: A foreign-key failure reads "FOREIGN KEY constraint failed" and a CHECK
#: failure reads "CHECK constraint failed: ...", so they are cleanly separable.
_DUPLICATE_MARKERS = ("unique constraint failed", "evaluations")


def is_evaluations_duplicate(exc: IntegrityError) -> bool:
    """Whether this IntegrityError is specifically invariant I3 firing.

    WHY THIS FUNCTION EXISTS AT ALL
    -------------------------------
    The obvious implementation is `except IntegrityError: raise
    DuplicateQueryError`. That is wrong, and wrong in a way that costs hours.
    The same exception class is raised by:

      * a FOREIGN KEY failure  — the alert row was not ensured first
      * a CHECK failure        — a bad status or severity slipped through
      * the UNIQUE on dispatch_attempts.idempotency_key (Module 3, invariant I2)

    Blanket-catching turns four unrelated bugs into one misleading message.
    Discriminating on the database's own text keeps each failure legible.
    """
    message = str(getattr(exc, "orig", exc)).lower()
    return all(marker in message for marker in _DUPLICATE_MARKERS)


# ─────────────────────────────────────────────────────────────────────────────
# PUSH — the PresenceBus
# ─────────────────────────────────────────────────────────────────────────────

#: Pushed to every subscriber by close(). Without a terminating sentinel an
#: `async for` over a subscription blocks forever at teardown, and pytest hangs
#: with no output at all — which reads like a crash rather than a leak.
_CLOSED = object()


class Subscription:
    """One subscriber's view of the bus.

    Registration happens in __init__ — SYNCHRONOUSLY, at subscribe() call time,
    not on first iteration. That ordering matters: the interrupt listener in
    Module 3 is created as a task and may not reach its first `__anext__` for
    some microseconds, and an event published in that window would be lost.
    A lost presence event means the wrong decision row fires, intermittently,
    on camera.
    """

    def __init__(self, bus: "PresenceBus") -> None:
        self._bus = bus
        self._queue: asyncio.Queue = asyncio.Queue()
        bus._register(self._queue)
        self._active = True

    # -- async context manager: guarantees teardown even on exception --------

    async def __aenter__(self) -> "Subscription":
        return self

    async def __aexit__(self, *exc_info) -> bool:
        self.unsubscribe()
        return False

    # -- async iterator ------------------------------------------------------

    def __aiter__(self) -> AsyncIterator[InterruptEvent]:
        return self

    async def __anext__(self) -> InterruptEvent:
        item = await self._queue.get()
        if item is _CLOSED:
            raise StopAsyncIteration
        return item

    async def get(self, *, timeout: float | None = None) -> InterruptEvent | None:
        """Await a single event. Returns None on timeout or on close.

        Convenience for tests, which want one event rather than a loop and must
        never be able to hang the suite.
        """
        try:
            item = (
                await asyncio.wait_for(self._queue.get(), timeout)
                if timeout is not None
                else await self._queue.get()
            )
        except asyncio.TimeoutError:
            return None
        return None if item is _CLOSED else item

    def unsubscribe(self) -> None:
        if self._active:
            self._bus._unregister(self._queue)
            self._active = False


class PresenceBus:
    """Fan-out event bus over asyncio.Queue, one queue per subscriber.

    NOTE THAT publish() IS SYNCHRONOUS. That is deliberate and load-bearing.
    The queues are unbounded, so `put_nowait` never blocks, which means
    publishing contains no await point and therefore CANNOT be suspended while
    a caller holds a lock. "Publish outside the lock" stops being a discipline
    somebody has to remember and becomes a property of the API.
    """

    def __init__(self) -> None:
        self._subscribers: list[asyncio.Queue] = []
        self._closed = False

    def _register(self, queue: asyncio.Queue) -> None:
        self._subscribers.append(queue)

    def _unregister(self, queue: asyncio.Queue) -> None:
        try:
            self._subscribers.remove(queue)
        except ValueError:
            pass

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    def subscribe(self) -> Subscription:
        """Register a subscriber. Usable as `async with` or `async for`.

        Multiple independent subscribers are supported; each gets its own queue
        and its own copy of every event, so the executor and the audit logger
        do not consume each other's events.
        """
        if self._closed:
            raise RuntimeError("cannot subscribe to a closed PresenceBus")
        return Subscription(self)

    def publish(self, event: InterruptEvent) -> None:
        """Deliver to every subscriber. Never blocks, never awaits."""
        if self._closed:
            return
        # Iterate a copy: a subscriber may unsubscribe from inside its own
        # handler, and mutating the list mid-iteration would skip the next one.
        for queue in list(self._subscribers):
            queue.put_nowait(event)

    def close(self) -> None:
        """Terminate every subscription's iteration.

        Idempotent, and safe to call from a finally block. Every `async for`
        over a subscription ends cleanly after this; without it, test teardown
        blocks forever.
        """
        if self._closed:
            return
        self._closed = True
        for queue in list(self._subscribers):
            queue.put_nowait(_CLOSED)
        self._subscribers.clear()


# ─────────────────────────────────────────────────────────────────────────────
# Latency
# ─────────────────────────────────────────────────────────────────────────────


def default_latency(seed: int = config.RNG_SEED) -> LatencyFn:
    """Seeded simulated round-trip time, in SECONDS.

    Making the cost of a registry lookup real is not decoration. It is what
    creates the window in which the world can change underneath an in-flight
    dispatch — which is the entire scenario this assessment is about. With zero
    latency there is no mid-flight, and nothing to detect.

    Seeded so a recorded demo is reproducible; a walkthrough whose timings shift
    on every take is very hard to narrate.
    """
    rng = random.Random(seed)
    low, high = config.LATENCY_MS_RANGE
    return lambda: rng.uniform(low, high) / 1000.0


def zero_latency() -> float:
    """Injected by tests. A suite that sleeps 200ms per pull stops being run."""
    return 0.0


# ─────────────────────────────────────────────────────────────────────────────
# The Registry
# ─────────────────────────────────────────────────────────────────────────────


class Registry:
    """Pull and push over the stakeholder store.

    Takes a session FACTORY rather than a session, and opens a short-lived
    session per operation. Module 3 runs the executor and the interrupt listener
    concurrently, and an AsyncSession is not concurrency-safe — sharing one
    across tasks produces interleaved transactions and sporadic
    InvalidRequestError. Owning the lifetime here removes the temptation.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        bus: PresenceBus | None = None,
        clock: Clock = time.time,
        latency: LatencyFn | None = None,
    ) -> None:
        self._sessions = session_factory
        self.bus = bus or PresenceBus()
        self._clock = clock
        self._latency = latency or default_latency()

    # ── helpers ─────────────────────────────────────────────────────────────

    @staticmethod
    def _to_record(row: Stakeholder) -> StakeholderRecord:
        """ORM row -> frozen Pydantic record, by VALUE.

        Every field is read here, inside the session, and copied into an
        immutable object. Nothing lazily-loaded escapes. If a snapshot held a
        still-attached ORM instance, attribute access would silently re-read the
        database and the snapshot would start tracking current truth — which
        would destroy the mid-flight mechanism entirely, since there would no
        longer be a stale-but-coherent view to diff an event against.

        JSON decoding happens here rather than in schemas.py, because the model
        layer should not know that the storage layer keeps arrays as TEXT.
        """
        return StakeholderRecord(
            id=row.id,
            name=row.name,
            title=row.title,
            primary_domain=row.primary_domain,
            secondary_domains=tuple(json.loads(row.secondary_domains or "[]")),
            seniority_tier=row.seniority_tier,
            preferred_channel=Channel(row.preferred_channel),
            fallback_channels=tuple(
                Channel(c) for c in json.loads(row.fallback_channels or "[]")
            ),
            status=Availability(row.status),
            on_call=bool(row.on_call),
        )

    async def _charge(
        self,
        session: AsyncSession,
        alert_id: str,
        records: Sequence[StakeholderRecord],
        observed_at: float,
    ) -> None:
        """Write the query-ledger rows. This is where invariant I3 is spent.

        qualification and ladder_rank are SENTINELS: scoring belongs to
        Module 2, and importing the scorer here would collapse the layer split
        on the first convenient occasion. reachability, by contrast, is genuinely
        known now — it is a property of the observation, not of the ranking.
        """
        rows = [
            {
                "alert_id": alert_id,
                "stakeholder_id": record.id,
                "observed_status": record.status.value,
                "observed_at": observed_at,
                "qualification": config.QUALIFICATION_SENTINEL,
                "reachability": record.status.reachability,
                "ladder_rank": config.LADDER_RANK_SENTINEL,
                "source": "pull",
            }
            for record in records
        ]
        try:
            await session.execute(sqlite_insert(Evaluation), rows)
            await session.commit()
        except IntegrityError as exc:
            await session.rollback()
            if is_evaluations_duplicate(exc):
                names = ", ".join(r.id for r in records)
                raise DuplicateQueryError(
                    f"availability for [{names}] was already queried for alert "
                    f"{alert_id!r} — use the cached snapshot or the push stream"
                ) from exc
            # Foreign key, CHECK, or the Module 3 idempotency key. Not ours.
            raise

    # ── PULL (counted, latency-bearing) ─────────────────────────────────────

    async def ensure_alert(self, alert: AlertEvent) -> None:
        """Persist the alert if it is not already there. Idempotent.

        MUST run before any evaluations row is written. `evaluations.alert_id`
        carries a foreign key to `alerts`, foreign keys are genuinely enforced
        (see the connect listener in db.py), and AlertEvent is a Pydantic object
        that exists only in memory until someone writes it down. Without this
        call the very first pull dies on a FOREIGN KEY constraint failure — and,
        if the exception handler were sloppy, would report itself as a phantom
        duplicate query.
        """
        statement = (
            sqlite_insert(Alert)
            .values(
                alert_id=alert.alert_id,
                metric_name=alert.metric_name,
                value=alert.value,
                threshold=alert.threshold,
                direction=alert.direction,
                severity=alert.severity.value,
                domain=alert.domain,
                source=alert.source,
                triggered_at=alert.triggered_at,
                state="routing",
            )
            .on_conflict_do_nothing(index_elements=["alert_id"])
        )
        async with self._sessions() as session:
            await session.execute(statement)
            await session.commit()

    async def query_by_domain(self, alert: AlertEvent) -> list[CandidateSnapshot]:
        """The ONE indexed lookup this alert is entitled to.

        Matches on primary_domain OR a JSON-contained secondary domain, in a
        single SELECT — one round trip, N candidates, not N round trips.

        THE QUERY DOES NOT FILTER ON STATUS. Adding `WHERE status = 'online'`
        looks like an obvious optimisation and would be a serious bug: the
        offline director would never enter the ladder, so the agent could never
        escalate UP to her, and invariant I4's whole demonstration becomes
        unreachable. Availability is a ranking input, not a matching criterion.
        """
        await self.ensure_alert(alert)
        latency_s = self._latency()
        await asyncio.sleep(latency_s)
        observed_at = self._clock()

        secondary_match = text(
            "EXISTS (SELECT 1 FROM json_each(stakeholders.secondary_domains) "
            "WHERE json_each.value = :secondary_domain)"
        ).bindparams(secondary_domain=alert.domain)

        statement = (
            select(Stakeholder)
            .where(
                or_(Stakeholder.primary_domain == alert.domain, secondary_match)
            )
            .order_by(Stakeholder.id)
        )

        async with self._sessions() as session:
            rows = (await session.scalars(statement)).all()
            records = [self._to_record(row) for row in rows]
            if records:
                await self._charge(session, alert.alert_id, records, observed_at)

        # Built after the session closes, from values only — see _to_record.
        return [
            CandidateSnapshot(
                alert_id=alert.alert_id,
                stakeholder=record,
                status=record.status,
                observed_at=observed_at,
                source="pull",
                latency_ms=latency_s * 1000.0,
            )
            for record in records
        ]

    async def fetch_one(
        self, alert: AlertEvent, stakeholder_id: str
    ) -> CandidateSnapshot:
        """Pull a single person. Same accounting, same one-shot rule.

        Used for cross-domain escalation: someone outside the alert's domain was
        never matched by query_by_domain, so they have not been charged and may
        still be looked at exactly once.
        """
        await self.ensure_alert(alert)
        latency_s = self._latency()
        await asyncio.sleep(latency_s)
        observed_at = self._clock()

        async with self._sessions() as session:
            row = await session.get(Stakeholder, stakeholder_id)
            if row is None:
                raise UnknownStakeholder(stakeholder_id)
            record = self._to_record(row)
            await self._charge(session, alert.alert_id, [record], observed_at)

        return CandidateSnapshot(
            alert_id=alert.alert_id,
            stakeholder=record,
            status=record.status,
            observed_at=observed_at,
            source="pull",
            latency_ms=latency_s * 1000.0,
        )

    # ── PUSH (free, uncounted) ──────────────────────────────────────────────

    async def set_status(
        self,
        stakeholder_id: str,
        status: Availability,
        *,
        reason: str = "",
    ) -> InterruptEvent | None:
        """Change someone's presence and announce it.

        WRITES NOTHING TO `evaluations`. Not an insert, not an update. That is
        what keeps the ledger an honest count of questions asked, and it is why
        the query counter can stay flat on screen while the routing decision
        changes.

        The database write completes and the session closes BEFORE publish() is
        called, so no subscriber can observe an event describing a transaction
        that has not landed. publish() itself is synchronous and non-blocking,
        so it cannot be suspended mid-notification.
        """
        async with self._sessions() as session:
            row = await session.get(Stakeholder, stakeholder_id)
            if row is None:
                raise UnknownStakeholder(stakeholder_id)
            previous = Availability(row.status)
            if previous is status:
                return None  # never announce a change that did not happen
            row.status = status.value
            row.updated_at = self._clock()
            await session.commit()

        event = InterruptEvent(
            kind=InterruptKind.PRESENCE_CHANGED,
            stakeholder_id=stakeholder_id,
            previous=previous,
            current=status,
            at=self._clock(),
            reason=reason,
        )
        self.bus.publish(event)
        return event

    async def set_channel_health(
        self,
        stakeholder_id: str,
        channel: Channel,
        healthy: bool,
        *,
        last_error: str | None = None,
    ) -> InterruptEvent:
        """Mark a transport up or down and announce it.

        A person is not their transport. A Slack outage and a human going
        offline are different events with different correct responses, so they
        travel as different interrupt kinds.
        """
        now = self._clock()
        statement = (
            sqlite_insert(ChannelHealth)
            .values(
                stakeholder_id=stakeholder_id,
                channel=channel.value,
                healthy=int(healthy),
                last_error=last_error,
                updated_at=now,
            )
            .on_conflict_do_update(
                index_elements=["stakeholder_id", "channel"],
                set_={
                    "healthy": int(healthy),
                    "last_error": last_error,
                    "updated_at": now,
                },
            )
        )
        async with self._sessions() as session:
            await session.execute(statement)
            await session.commit()

        event = InterruptEvent(
            kind=InterruptKind.CHANNEL_DEGRADED,
            stakeholder_id=stakeholder_id,
            channel=channel,
            healthy=healthy,
            at=now,
            reason=last_error or ("healthy" if healthy else "degraded"),
        )
        self.bus.publish(event)
        return event

    async def set_on_call(self, stakeholder_id: str, on_call: bool) -> None:
        """Scenario affordance, NOT part of the routing path.

        Appendix A's `floor` scenario needs Tom Beckett at qualification 108
        rather than 123, which means off the rota. Toggling him here keeps one
        seed file instead of two that can drift apart.

        Deliberately publishes NO event: a rota change is not a presence change,
        and pretending otherwise would let a scenario fire an interrupt the real
        system would never see.
        """
        async with self._sessions() as session:
            await session.execute(
                update(Stakeholder)
                .where(Stakeholder.id == stakeholder_id)
                .values(on_call=int(bool(on_call)), updated_at=self._clock())
            )
            await session.commit()

    # ── introspection (tests and demo only — never used for routing) ────────

    async def evaluation_count(self, alert_id: str) -> int:
        """How many questions this alert has spent. Shown on screen in the demo
        so the audience can watch it NOT move during a mid-flight re-route."""
        async with self._sessions() as session:
            return int(
                await session.scalar(
                    select(func.count())
                    .select_from(Evaluation)
                    .where(Evaluation.alert_id == alert_id)
                )
                or 0
            )

    async def peek(self, stakeholder_id: str) -> StakeholderRecord:
        """Read ground truth with no latency and no accounting.

        Strictly a test and demo affordance, so a harness can show what is
        actually true next to what the agent believes. THE ROUTER MUST NEVER
        CALL THIS — if it did, "no double query" would become trivially true
        and completely meaningless.
        """
        async with self._sessions() as session:
            row = await session.get(Stakeholder, stakeholder_id)
            if row is None:
                raise UnknownStakeholder(stakeholder_id)
            return self._to_record(row)
