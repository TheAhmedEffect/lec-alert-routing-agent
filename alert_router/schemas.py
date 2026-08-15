"""
The frozen contracts. Every model that carries alert context is immutable.

WHY frozen=True IS AN INVARIANT AND NOT A STYLE CHOICE
------------------------------------------------------
Invariant I1 says a re-route never loses the alert. The cheapest way to lose it
is for something downstream — a channel adapter, a renderer, a decision handler
— to "helpfully" mutate the object it was handed. Freezing the models makes
that impossible rather than merely discouraged: the failure moves from a subtle
wrong-context bug at 3am to a TypeError at the moment the bad code is written.

The same reasoning drives tuples instead of lists. A frozen model containing a
list is only shallowly immutable — the list is still mutable, and the model is
no longer hashable. Tuples close both holes.

CandidateSnapshot deserves special attention. It is a POINT-IN-TIME OBSERVATION,
not a view onto the registry. It must never refresh itself, because the gap
between what it says and what is currently true IS the mid-flight signal this
whole system is built to detect.
"""

from __future__ import annotations

import time
from enum import Enum
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from . import config

#: Shared config for every context-carrying model. See the module docstring.
FROZEN = ConfigDict(frozen=True)


# ─────────────────────────────────────────────────────────────────────────────
# Enums — kept in lockstep with the database CHECK constraints
# ─────────────────────────────────────────────────────────────────────────────
# These are declared explicitly rather than generated from config, because
# generated enums lose their properties, their docstrings and their type
# information. The consistency assertion at the bottom of this section is what
# stops them drifting from the CHECK constraints in models_orm.py — if someone
# adds a channel here and forgets the database, the package fails to import
# rather than failing at 3am on a write.


class Severity(str, Enum):
    LOW = "low"
    HIGH = "high"
    CRITICAL = "critical"

    @property
    def rank(self) -> int:
        return {"low": 1, "high": 2, "critical": 3}[self.value]


class Availability(str, Enum):
    ONLINE = "online"
    BUSY = "busy"
    OFFLINE = "offline"

    @property
    def reachability(self) -> int:
        """2 / 1 / 0 — a SEPARATE axis from qualification.

        This number is the only place availability is ever allowed to influence
        ranking. It is deliberately not expressible as points, because points
        would be addable to a qualification score, and the moment that happens
        an online junior can out-rank an offline director. That is invariant I4
        breaking, and it is the specific failure the brief warns about.
        """
        return {"online": 2, "busy": 1, "offline": 0}[self.value]

    @property
    def is_reachable(self) -> bool:
        return self.reachability > 0


class Channel(str, Enum):
    SLACK = "slack"
    EMAIL = "email"
    SMS = "sms"

    @property
    def is_persistent(self) -> bool:
        """Email and SMS survive the recipient being offline; Slack does not.

        This one property is why a presence drop mid-SMS is a NON-EVENT
        (decision matrix row R2). "Offline" means away from keyboard, not
        unreachable — the message waits on the device. Treating the two as the
        same thing causes a whole class of pointless re-routes.
        """
        return self is not Channel.SLACK


class AttemptState(str, Enum):
    RESERVED = "reserved"
    CONNECTING = "connecting"
    IN_FLIGHT = "in_flight"
    COMMITTED = "committed"
    ABORTED = "aborted"
    FAILED = "failed"

    @property
    def is_terminal(self) -> bool:
        return self in (AttemptState.COMMITTED, AttemptState.ABORTED, AttemptState.FAILED)

    @property
    def is_pre_commit(self) -> bool:
        """True while the attempt can still be cleanly abandoned.

        The decision matrix separates rows R2 and R5 on exactly this predicate,
        so it lives here rather than being re-derived at each call site.
        """
        return self in (
            AttemptState.RESERVED,
            AttemptState.CONNECTING,
            AttemptState.IN_FLIGHT,
        )


def _assert_enums_match_config() -> None:
    """Fail loudly at import if an enum and its CHECK constraint disagree.

    The database and the application must have exactly one shared idea of what
    a legal value is. Without this, adding Channel.TEAMS here would produce a
    system that writes a value the CHECK constraint rejects — discovered on a
    real dispatch rather than at startup.
    """
    pairs = (
        ("CHANNEL_VALUES", Channel, config.CHANNEL_VALUES),
        ("STATUS_VALUES", Availability, config.STATUS_VALUES),
        ("SEVERITY_VALUES", Severity, config.SEVERITY_VALUES),
        ("ATTEMPT_STATE_VALUES", AttemptState, config.ATTEMPT_STATE_VALUES),
    )
    for name, enum_cls, expected in pairs:
        actual = tuple(member.value for member in enum_cls)
        if set(actual) != set(expected):
            raise RuntimeError(
                f"schemas.{enum_cls.__name__} is out of sync with config.{name}: "
                f"{sorted(actual)} != {sorted(expected)}"
            )


_assert_enums_match_config()


# ─────────────────────────────────────────────────────────────────────────────
# The alert
# ─────────────────────────────────────────────────────────────────────────────


class AlertEvent(BaseModel):
    """A threshold breach. Immutable, and carried by reference for its lifetime.

    Invariant I1 means the object the final recipient sees is THIS object, not
    a reconstruction of it. Anywhere in the pipeline that builds a new
    AlertEvent from an old one's fields is a bug.
    """

    model_config = FROZEN

    alert_id: str = Field(default_factory=lambda: f"alr-{uuid4().hex[:8]}")
    metric_name: str
    value: float
    threshold: float
    direction: Literal["above", "below"] = "above"
    severity: Severity
    domain: str
    source: str = "metrics-pipeline"
    triggered_at: float = Field(default_factory=time.time)

    @model_validator(mode="after")
    def _must_actually_breach(self):
        """Refuse an alert that does not breach its own threshold.

        `direction` exists because depletion metrics — stock levels, contract
        expiry, remaining credit — breach by FALLING. A router that hardcodes
        `value > threshold` drops a stock-out silently, which is the worst
        possible failure for an alerting system: no signal, no error, no record.

        Validating here rather than at the routing layer means a malformed
        alert cannot enter the system at all.
        """
        breached = (
            self.value > self.threshold
            if self.direction == "above"
            else self.value < self.threshold
        )
        if not breached:
            raise ValueError(
                f"{self.metric_name}={self.value} does not breach "
                f"{self.threshold} ({self.direction})"
            )
        return self

    @property
    def breach_magnitude(self) -> float:
        """How far past the threshold, proportionally. Used for explanation text."""
        return abs(self.value - self.threshold) / abs(self.threshold or 1)

    def describe(self) -> str:
        arrow = ">" if self.direction == "above" else "<"
        return (
            f"{self.metric_name} {self.value:g} {arrow} {self.threshold:g} "
            f"[{self.severity.value}/{self.domain}]"
        )


# ─────────────────────────────────────────────────────────────────────────────
# The registry
# ─────────────────────────────────────────────────────────────────────────────


class StakeholderRecord(BaseModel):
    """A person, as known to the router.

    Domains are ordered: `primary_domain` is the specialism, `secondary_domains`
    is adjacent competence. Ranking scores those differently, and that gap is
    what lets a primary-domain L3 out-rank a secondary-domain L4.
    """

    model_config = FROZEN

    id: str
    name: str
    title: str = ""
    primary_domain: str
    # Tuples, not lists: a frozen model holding a list is only shallowly
    # immutable and is not hashable.
    secondary_domains: tuple[str, ...] = ()
    seniority_tier: int = Field(
        ge=config.SENIORITY_TIER_MIN, le=config.SENIORITY_TIER_MAX
    )
    preferred_channel: Channel
    fallback_channels: tuple[Channel, ...] = ()
    status: Availability
    on_call: bool = False

    def domain_fit(self, domain: str) -> Literal["primary", "secondary", "none"]:
        if domain == self.primary_domain:
            return "primary"
        return "secondary" if domain in self.secondary_domains else "none"

    @property
    def channel_order(self) -> tuple[Channel, ...]:
        """Preferred first, then fallbacks. Order is meaningful — it is the
        sequence row R6 walks during a channel failover."""
        return (self.preferred_channel, *self.fallback_channels)


class CandidateSnapshot(BaseModel):
    """What ONE pull returned about ONE person, at ONE moment.

    `status` is what was true at `observed_at` and is never silently refreshed.
    That is the entire point: the divergence between this and current reality is
    the mid-flight signal. If this model tracked the database, there would be no
    change left to detect and nothing for a push event to tell us.

    A push produces a NEW snapshot with source='push' via
    DispatchState.patch_from_push (Module 2), so the audit trail records not
    just what we knew but how we came to know it.
    """

    model_config = FROZEN

    alert_id: str
    stakeholder: StakeholderRecord
    status: Availability
    observed_at: float
    source: Literal["pull", "push"] = "pull"
    latency_ms: float = 0.0

    @property
    def reachability(self) -> int:
        return self.status.reachability


# ─────────────────────────────────────────────────────────────────────────────
# Scoring and the ladder  (populated by Module 2; declared here so the
# contracts live in one file)
# ─────────────────────────────────────────────────────────────────────────────


class ScoreBreakdown(BaseModel):
    """Every term kept separate so the notification can say 'chosen over Marcus
    because primary domain 100 versus secondary 55'.

    `qualification` is the sum of the three point terms and NOTHING ELSE. There
    is deliberately no availability term. `reachability` sits beside it as an
    independent key.
    """

    model_config = FROZEN

    domain_points: float
    seniority_points: float
    on_call_points: float
    qualification: float
    reachability: int
    eligible: bool
    notes: tuple[str, ...] = ()


class RankedCandidate(BaseModel):
    model_config = FROZEN

    snapshot: CandidateSnapshot
    score: ScoreBreakdown
    rank: int


class DispatchPlan(BaseModel):
    """Frozen at ranking time.

    Ladder MEMBERSHIP never grows without a new pull, which is what makes I3
    structural rather than aspirational. Scores may be recomputed from patched
    observations; members may not be added.
    """

    model_config = FROZEN

    alert: AlertEvent
    ladder: tuple[RankedCandidate, ...]
    created_at: float
    plan_version: int = 1


# ─────────────────────────────────────────────────────────────────────────────
# Attempts and audit
# ─────────────────────────────────────────────────────────────────────────────


class AttemptRecord(BaseModel):
    """One attempt to reach one person.

    This is the ONE deliberately mutable model in the system: an attempt walks
    a state machine, so it has to change. Every mutation happens under the
    per-alert DispatchState lock, which is why there is exactly one place
    concurrent writes can go wrong.
    """

    model_config = ConfigDict(frozen=False)

    alert_id: str
    stakeholder_id: str
    channel: Channel
    role: Literal["primary", "reroute", "escalation", "fyi"]
    state: AttemptState = AttemptState.RESERVED
    plan_version: int = 1
    reserved_at: float
    committed_at: float | None = None
    outcome_reason: str = ""
    #: Set when WE cancelled deliberately, so the executor can tell a chosen
    #: abort apart from a real shutdown and know whether to re-raise.
    abort_requested: bool = False

    @property
    def idempotency_key(self) -> str:
        """'{alert_id}:{stakeholder_id}' — the CHANNEL IS EXCLUDED.

        A failover from Slack to SMS is the same notification down a different
        pipe, so it must collide with itself and be treated as a continuation.
        Including the channel here would silently permit exactly the duplicate
        that invariant I2 forbids.
        """
        return f"{self.alert_id}:{self.stakeholder_id}"


class AuditEvent(BaseModel):
    """One line of the incident narrative. Append-only, never updated."""

    model_config = FROZEN

    alert_id: str
    seq: int
    at: float
    kind: str
    actor: str
    summary: str
    payload: dict = Field(default_factory=dict)

    def line(self) -> str:
        return f"[{self.seq:02d}] {self.kind:<10} {self.actor:<12} {self.summary}"


# ─────────────────────────────────────────────────────────────────────────────
# Interrupts — the push side of the world
# ─────────────────────────────────────────────────────────────────────────────


class InterruptKind(str, Enum):
    PRESENCE_CHANGED = "presence_changed"
    CHANNEL_DEGRADED = "channel_degraded"
    #: DERIVED by the interrupt listener from patched observations, never
    #: published raw by the registry. Deriving it is free; discovering it would
    #: require a pull, which the query budget forbids.
    BETTER_MATCH = "better_match"


class InterruptEvent(BaseModel):
    """A push notification about the world changing.

    THE PAYLOAD CARRIES THE NEW STATE. That is the mechanism that makes
    "detect a change without re-querying" satisfiable at all: we never ask what
    someone's status is now, we are told, and we patch our cached observation
    from what we were told.
    """

    model_config = FROZEN

    kind: InterruptKind
    stakeholder_id: str
    previous: Availability | None = None
    current: Availability | None = None
    channel: Channel | None = None
    healthy: bool | None = None
    at: float
    reason: str = ""

    @property
    def went_offline(self) -> bool:
        return bool(
            self.previous
            and self.current
            and self.previous.reachability > 0
            and self.current.reachability == 0
        )

    @property
    def came_online(self) -> bool:
        return bool(
            self.previous
            and self.current
            and self.previous.reachability == 0
            and self.current.reachability > 0
        )


# ─────────────────────────────────────────────────────────────────────────────
# Decisions and the final payload  (Modules 4 and 5)
# ─────────────────────────────────────────────────────────────────────────────


class DecisionAction(str, Enum):
    CONTINUE_UNCHANGED = "continue_unchanged"
    CHANNEL_FAILOVER = "channel_failover"
    ABORT_AND_REROUTE = "abort_and_reroute"
    COMPLETE_AND_ESCALATE_PARALLEL = "complete_and_escalate_parallel"
    HOLD_AND_ESCALATE_UP = "hold_and_escalate_up"
    EXHAUSTED = "exhausted"


class RoutingDecision(BaseModel):
    model_config = FROZEN

    action: DecisionAction
    target_id: str | None = None
    escalate_to_id: str | None = None
    rationale: str
    #: 'R4' — traceable to the decision matrix. Present on EVERY decision,
    #: including the boring CONTINUE_UNCHANGED ones, so the audit trail can be
    #: read against the table.
    matrix_row: str
    suppressed: tuple[tuple[str, str], ...] = ()


class NotificationEnvelope(BaseModel):
    """Everything the recipient gets. This model IS invariant I1.

    It carries the ORIGINAL AlertEvent — not a copy — plus the complete audit
    trail and the numeric comparison that justifies this person over the
    runner-up.
    """

    model_config = FROZEN

    alert: AlertEvent
    recipient: StakeholderRecord
    channel: Channel
    role: Literal["primary", "reroute", "escalation", "fyi"]
    plan_version: int
    chosen_because: str
    considered_and_passed: tuple[tuple[str, str], ...] = ()
    audit_trail: tuple[str, ...] = ()
    rendered_body: str = ""
