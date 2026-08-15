"""
DispatchState — the one mutable object per alert — and the ladder write-back.

WHY THERE IS EXACTLY ONE MUTABLE THING
--------------------------------------
Everything this object HOLDS is immutable: frozen snapshots, a frozen plan,
frozen audit events. So there is exactly one place in the whole system where
concurrent writes can go wrong, and exactly one lock protecting it. In Module 3
the executor and the interrupt listener run as separate tasks against this same
object; that is only tractable because the surface is this small.

THE LEDGERS ARE DISJOINT ON PURPOSE
-----------------------------------
    evaluated  — did we ASK about them?      (the query budget, invariant I3)
    attempted  — did we TRY to reach them?   (includes ABORTED, invariant I2)
    notified   — did a message actually LAND? (committed only)
    suppressed — did we deliberately pass over them, and with what arithmetic?

Collapsing any two of these is how duplicates appear. "We tried" and "they got
it" are different facts, and an aborted attempt is emphatically the first
without being the second — yet it must still block a retry, because the
idempotency key was taken at reservation.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Callable, Iterable, Sequence

from sqlalchemy import insert, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .models_orm import AuditEvent as AuditEventRow
from .models_orm import Evaluation
from .ranking import build_ladder, score
from .schemas import (
    AlertEvent,
    AttemptRecord,
    AttemptState,
    AuditEvent,
    CandidateSnapshot,
    DispatchPlan,
    InterruptEvent,
    RankedCandidate,
)

Clock = Callable[[], float]
SessionFactory = async_sessionmaker[AsyncSession]


# ─────────────────────────────────────────────────────────────────────────────
# Ladder write-back
# ─────────────────────────────────────────────────────────────────────────────


async def persist_ladder(session_factory: SessionFactory, plan: DispatchPlan) -> int:
    """Fill in the sentinels Module 1 wrote at pull time.

    UPDATE, NEVER INSERT. The evaluations rows already exist — the pull created
    them with qualification=0.0 and ladder_rank=-1 because scoring had not
    happened yet. An INSERT here would collide with the composite primary key
    and surface as DuplicateQueryError, which would look like an invariant I3
    violation when it is really a write-back bug. That misdiagnosis is expensive,
    so it is worth the comment.

    `source` is deliberately left alone. Those rows were paid for by a pull and
    stay labelled 'pull' forever; ranking is a derivation, not a new observation.
    """
    async with session_factory() as session:
        for candidate in plan.ladder:
            await session.execute(
                update(Evaluation)
                .where(
                    Evaluation.alert_id == plan.alert.alert_id,
                    Evaluation.stakeholder_id == candidate.snapshot.stakeholder.id,
                )
                .values(
                    qualification=candidate.score.qualification,
                    ladder_rank=candidate.rank,
                )
            )
        await session.commit()
    return len(plan.ladder)


# ─────────────────────────────────────────────────────────────────────────────
# DispatchState
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class DispatchState:
    alert: AlertEvent
    plan: DispatchPlan

    session_factory: SessionFactory | None = None
    clock: Clock = time.time
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    evaluated: set[str] = field(default_factory=set)
    observed: dict[str, CandidateSnapshot] = field(default_factory=dict)
    attempted: dict[str, AttemptRecord] = field(default_factory=dict)
    notified: set[str] = field(default_factory=set)
    suppressed: dict[str, str] = field(default_factory=dict)

    current_attempt: AttemptRecord | None = None
    plan_version: int = 1
    audit: list[AuditEvent] = field(default_factory=list)
    terminal: bool = False

    _seq: int = 0

    # ── construction ────────────────────────────────────────────────────────

    @classmethod
    def start(
        cls,
        alert: AlertEvent,
        snapshots: Sequence[CandidateSnapshot],
        *,
        session_factory: SessionFactory | None = None,
        clock: Clock = time.time,
    ) -> "DispatchState":
        """Build the frozen ladder and seed the ledgers from ONE pull.

        `evaluated` is the in-memory mirror of the evaluations table. It exists
        so the hot path never has to ask the database "have we already looked at
        this person?" — that answer is already paid for and already known.
        """
        plan = build_ladder(snapshots, alert, clock=clock)
        state = cls(
            alert=alert, plan=plan, session_factory=session_factory, clock=clock
        )
        for snapshot in snapshots:
            state.evaluated.add(snapshot.stakeholder.id)
            state.observed[snapshot.stakeholder.id] = snapshot
        return state

    # ── knowledge, without questions ────────────────────────────────────────

    def patch_from_push(self, event: InterruptEvent) -> bool:
        """Update what we know WITHOUT a query. Returns False if not ours.

        The event payload carries the new state, so no question is asked and the
        pull ledger never moves. This is the mechanism the whole assessment
        turns on.

        Returning False rather than raising matters: cross-domain presence
        traffic constantly names people this alert never evaluated, and
        indexing self.observed blindly would raise KeyError on entirely normal
        events. A stranger's status change is not ours to act on — that is
        decision-matrix row R1, arriving early.
        """
        previous = self.observed.get(event.stakeholder_id)
        if previous is None or event.current is None:
            return False
        self.observed[event.stakeholder_id] = previous.model_copy(
            update={
                "status": event.current,
                "observed_at": event.at,
                "source": "push",
            }
        )
        return True

    def rescore(self) -> DispatchPlan:
        """Recompute scores from PATCHED observations. Membership and rank frozen.

        Re-scoring is legal because it uses facts already paid for. Re-resolving
        — adding somebody who was never evaluated — would require a new pull, so
        it is forbidden, and this method structurally cannot do it: it iterates
        the existing ladder and preserves each candidate's rank.

        A pleasant consequence, and a good thing to point at in the walkthrough:
        a presence change moves `reachability` and leaves `qualification`
        untouched, because qualification has no availability term. Invariant I4
        is visible in the diff of a rescore.
        """
        updated = tuple(
            RankedCandidate(
                snapshot=self.observed.get(
                    candidate.snapshot.stakeholder.id, candidate.snapshot
                ),
                score=score(
                    self.observed.get(
                        candidate.snapshot.stakeholder.id, candidate.snapshot
                    ),
                    self.alert,
                ),
                rank=candidate.rank,  # frozen: order is decided once
            )
            for candidate in self.plan.ladder
        )
        self.plan = self.plan.model_copy(update={"ladder": updated})
        return self.plan

    # ── walking the ladder ──────────────────────────────────────────────────

    def next_candidate(self) -> RankedCandidate | None:
        """The next person to try, in frozen rank order.

        Skips anyone in `attempted` — INCLUDING ABORTED ATTEMPTS — and anyone
        suppressed. Testing only `notified` here would let an aborted person be
        re-offered, and their idempotency key is already taken, so it would
        surface as an IntegrityError mid-reroute: a duplicate-looking error that
        is really a ladder-walk bug.
        """
        for candidate in self.plan.ladder:
            person_id = candidate.snapshot.stakeholder.id
            if person_id in self.attempted or person_id in self.suppressed:
                continue
            return candidate
        return None

    def candidate_for(self, stakeholder_id: str) -> RankedCandidate | None:
        for candidate in self.plan.ladder:
            if candidate.snapshot.stakeholder.id == stakeholder_id:
                return candidate
        return None

    def in_ladder(self, stakeholder_id: str) -> bool:
        return self.candidate_for(stakeholder_id) is not None

    @property
    def incumbent(self) -> RankedCandidate | None:
        """The candidate the current attempt is aimed at, if any."""
        if self.current_attempt is None:
            return None
        return self.candidate_for(self.current_attempt.stakeholder_id)

    @property
    def remaining(self) -> list[RankedCandidate]:
        """Everyone still eligible to be tried, in rank order."""
        return [
            candidate
            for candidate in self.plan.ladder
            if candidate.snapshot.stakeholder.id not in self.attempted
            and candidate.snapshot.stakeholder.id not in self.suppressed
        ]

    def mark_suppressed(self, stakeholder_id: str, why: str) -> None:
        """Record a deliberate pass-over, with the arithmetic that justified it.

        `why` must be the numeric string from clears_floor(). A suppression
        without a reason is indistinguishable from a bug, and this line ends up
        on screen in the walkthrough.

        No lock: a single dict assignment between await points is atomic under
        asyncio, and taking the lock here would invite callers to hold it across
        the audit write that usually follows.
        """
        self.suppressed[stakeholder_id] = why

    # ── audit ───────────────────────────────────────────────────────────────

    async def record_audit(
        self,
        kind: str,
        actor: str,
        summary: str,
        payload: dict | None = None,
    ) -> AuditEvent:
        """Append one line to the incident narrative. Never updates, never deletes.

        `seq` is allocated INSIDE the lock. In Module 3 the executor and the
        interrupt listener both write here, and audit_events carries
        UNIQUE(alert_id, seq) — so an unlocked increment would collide and take
        the whole dispatch down at the least convenient moment.

        The database write happens OUTSIDE the lock. Holding a lock across I/O
        would serialise the two tasks that are supposed to run concurrently, and
        it is unnecessary: seq was already allocated atomically, so rows may
        land out of order and still read back in the right order.
        """
        async with self.lock:
            seq = self._seq
            self._seq += 1
            event = AuditEvent(
                alert_id=self.alert.alert_id,
                seq=seq,
                at=self.clock(),
                kind=kind,
                actor=actor,
                summary=summary,
                payload=payload or {},
            )
            self.audit.append(event)

        if self.session_factory is not None:
            async with self.session_factory() as session:
                await session.execute(
                    insert(AuditEventRow).values(
                        alert_id=event.alert_id,
                        seq=event.seq,
                        at=event.at,
                        kind=event.kind,
                        actor=event.actor,
                        summary=event.summary,
                        payload=json.dumps(event.payload),
                    )
                )
                await session.commit()

        return event

    def audit_lines(self) -> tuple[str, ...]:
        """The narrative, in order. Goes verbatim into the NotificationEnvelope,
        which is how invariant I1 survives a re-route."""
        return tuple(event.line() for event in sorted(self.audit, key=lambda e: e.seq))

    # ── attempts ────────────────────────────────────────────────────────────

    def register_attempt(self, attempt: AttemptRecord) -> None:
        """Put an attempt in the `attempted` ledger and make it current.

        Called at RESERVE, before anything is sent — which is exactly why an
        abort still blocks a retry.
        """
        self.attempted[attempt.stakeholder_id] = attempt
        self.current_attempt = attempt

    def mark_notified(self, stakeholder_id: str) -> None:
        """Promote to the `notified` ledger. COMMITTED attempts only.

        Anything in here is a message that exists in the world and cannot be
        unsent, which is what makes decision row R5 (supplement, never retract)
        the only honest post-commit option.
        """
        self.notified.add(stakeholder_id)

    def summary(self) -> dict:
        """Compact snapshot for the CLI and for test failure messages."""
        return {
            "alert": self.alert.alert_id,
            "ladder": [c.snapshot.stakeholder.id for c in self.plan.ladder],
            "evaluated": sorted(self.evaluated),
            "attempted": {
                k: v.state.value for k, v in sorted(self.attempted.items())
            },
            "notified": sorted(self.notified),
            "suppressed": dict(sorted(self.suppressed.items())),
            "terminal": self.terminal,
        }
