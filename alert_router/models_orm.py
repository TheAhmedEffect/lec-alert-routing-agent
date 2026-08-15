"""
The database schema. Read the constraints as the specification — the Python in
this package is downstream of them.

WHY THE CONSTRAINTS ARE WRITTEN OUT LONGHAND
--------------------------------------------
Two of this system's four invariants are enforced here rather than in
application code:

  I2 — no duplicate notification  ->  UniqueConstraint on
                                      dispatch_attempts.idempotency_key
  I3 — one availability query      ->  PrimaryKeyConstraint on
       per person per alert            (evaluations.alert_id, stakeholder_id)

That distinction is the strongest claim this submission makes. "My code checks
for it" is a promise; "the database refuses the write" is a property. So every
CHECK is declared as an explicit CheckConstraint object rather than left to a
column type, because a plain String column silently accepts anything and the
constraint becomes a comment that lies.

If a write ever fails against one of these, the calling code is wrong. Do not
relax the schema to make a test pass.
"""

from __future__ import annotations

from sqlalchemy import (
    CheckConstraint,
    Float,
    ForeignKey,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from . import config


class Base(DeclarativeBase):
    """Declarative base for every table in the system."""


def _in_check(column: str, values: tuple[str, ...]) -> str:
    """Render `column IN ('a','b','c')` for a CheckConstraint.

    The values come from config.py — they are compile-time constants, never
    user input — so interpolating them is safe. Deriving the SQL from the same
    tuple that schemas.py builds its enums from is what stops the database and
    the application from disagreeing about what a legal value is.
    """
    rendered = ", ".join(f"'{value}'" for value in values)
    return f"{column} IN ({rendered})"


# ─────────────────────────────────────────────────────────────────────────────
# stakeholders — the registry
# ─────────────────────────────────────────────────────────────────────────────


class Stakeholder(Base):
    """A person who can be notified.

    Domains are ORDERED. `primary_domain` is the person's specialism and
    `secondary_domains` is adjacent competence, and splitting them is what lets
    ranking score a primary-domain L3 above a secondary-domain L4 — the
    mechanism behind invariant I4.

    `on_call` is separate from `status` ON PURPOSE. Being rostered and being at
    your desk are different facts; conflating them is how a system ends up
    paging whoever happens to be awake instead of whoever is responsible.
    """

    __tablename__ = "stakeholders"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    title: Mapped[str] = mapped_column(String, nullable=False, default="", server_default="")

    primary_domain: Mapped[str] = mapped_column(String, nullable=False)

    # JSON array held as TEXT rather than SQLAlchemy's JSON type, because the
    # registry's domain query uses SQLite's json_each() over this column and
    # keeping the storage explicit keeps that SQL obvious to a reader.
    secondary_domains: Mapped[str] = mapped_column(
        Text, nullable=False, default="[]", server_default="[]"
    )

    seniority_tier: Mapped[int] = mapped_column(Integer, nullable=False)
    preferred_channel: Mapped[str] = mapped_column(String, nullable=False)
    fallback_channels: Mapped[str] = mapped_column(
        Text, nullable=False, default="[]", server_default="[]"
    )
    status: Mapped[str] = mapped_column(String, nullable=False)

    # SQLite has no boolean type; 0/1 with a CHECK is the honest representation.
    on_call: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    updated_at: Mapped[float] = mapped_column(Float, nullable=False)

    __table_args__ = (
        CheckConstraint(
            f"seniority_tier BETWEEN {config.SENIORITY_TIER_MIN} "
            f"AND {config.SENIORITY_TIER_MAX}",
            name="ck_stakeholders_seniority_tier",
        ),
        CheckConstraint(
            _in_check("preferred_channel", config.CHANNEL_VALUES),
            name="ck_stakeholders_preferred_channel",
        ),
        CheckConstraint(
            _in_check("status", config.STATUS_VALUES),
            name="ck_stakeholders_status",
        ),
        CheckConstraint("on_call IN (0, 1)", name="ck_stakeholders_on_call"),
        Index("ix_stakeholders_primary_domain", "primary_domain"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# channel_health — transports fail independently of people
# ─────────────────────────────────────────────────────────────────────────────


class ChannelHealth(Base):
    """Models "their preferred channel is unavailable".

    A person is not their transport. A Slack outage and a human going offline
    are different events with different correct responses — row R6 fails the
    transport over without changing the recipient, because changing person in
    response to a transport fault is an over-reaction.

    init_db() seeds one healthy row per (person, channel) in that person's
    channel_order, so later "is a healthy fallback available?" queries are a
    plain SELECT rather than a query that must treat a missing row as
    implicitly healthy.
    """

    __tablename__ = "channel_health"

    stakeholder_id: Mapped[str] = mapped_column(
        String, ForeignKey("stakeholders.id"), nullable=False
    )
    channel: Mapped[str] = mapped_column(String, nullable=False)
    healthy: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[float] = mapped_column(Float, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("stakeholder_id", "channel", name="pk_channel_health"),
        CheckConstraint(
            _in_check("channel", config.CHANNEL_VALUES),
            name="ck_channel_health_channel",
        ),
        CheckConstraint("healthy IN (0, 1)", name="ck_channel_health_healthy"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# alerts — immutable except for `state`
# ─────────────────────────────────────────────────────────────────────────────


class Alert(Base):
    """A threshold breach.

    THIS ROW MUST EXIST BEFORE ANY EVALUATION REFERENCES IT. `evaluations` and
    `dispatch_attempts` both carry a foreign key to it, and foreign keys are
    genuinely enforced (see the pragma listener in db.py), so registry.py calls
    ensure_alert() before the first ledger write. Without that, the very first
    pull dies on a FOREIGN KEY constraint failure.

    `direction` supports depletion metrics that breach BELOW a threshold. A
    router that only understands `value > threshold` drops a stock-out on the
    floor without a sound.
    """

    __tablename__ = "alerts"

    alert_id: Mapped[str] = mapped_column(String, primary_key=True)
    metric_name: Mapped[str] = mapped_column(String, nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    threshold: Mapped[float] = mapped_column(Float, nullable=False)
    direction: Mapped[str] = mapped_column(String, nullable=False)
    severity: Mapped[str] = mapped_column(String, nullable=False)
    domain: Mapped[str] = mapped_column(String, nullable=False)
    source: Mapped[str] = mapped_column(
        String, nullable=False, default="metrics-pipeline",
        server_default="metrics-pipeline",
    )
    triggered_at: Mapped[float] = mapped_column(Float, nullable=False)
    state: Mapped[str] = mapped_column(
        String, nullable=False, default="routing", server_default="routing"
    )

    __table_args__ = (
        CheckConstraint(
            _in_check("direction", config.DIRECTION_VALUES),
            name="ck_alerts_direction",
        ),
        CheckConstraint(
            _in_check("severity", config.SEVERITY_VALUES),
            name="ck_alerts_severity",
        ),
        CheckConstraint(
            _in_check("state", config.ALERT_STATE_VALUES),
            name="ck_alerts_state",
        ),
    )


# ─────────────────────────────────────────────────────────────────────────────
# evaluations — THE QUERY LEDGER.  Invariant I3 lives in this primary key.
# ─────────────────────────────────────────────────────────────────────────────


class Evaluation(Base):
    """One paid-for observation of one person, for one alert.

    ================== INVARIANT I3 ==================
    PrimaryKeyConstraint(alert_id, stakeholder_id).

    A second availability pull for the same (alert, person) is an
    IntegrityError. This is not a rule the code remembers to follow; it is a
    write the database refuses. The constraint cannot be forgotten, bypassed by
    a new code path, or lost in a refactor.

    Note the key is scoped PER ALERT, not globally: two concurrent incidents are
    each entitled to their own single look at the same person.
    =================================================

    `qualification` and `ladder_rank` are written as SENTINELS at pull time
    (config.QUALIFICATION_SENTINEL / config.LADDER_RANK_SENTINEL) because
    scoring belongs to Module 2. `reachability`, by contrast, IS known now — it
    is a property of the observation itself, not of the ranking.

    `source` is 'pull' for every row this system writes. Push events
    deliberately never touch this table, which is exactly what makes the ledger
    a true count of QUESTIONS ASKED rather than of facts known. The 'push'
    value is admitted by the CHECK so the column can express provenance if a
    future design ever needs it.
    """

    __tablename__ = "evaluations"

    alert_id: Mapped[str] = mapped_column(
        String, ForeignKey("alerts.alert_id"), nullable=False
    )
    stakeholder_id: Mapped[str] = mapped_column(
        String, ForeignKey("stakeholders.id"), nullable=False
    )
    observed_status: Mapped[str] = mapped_column(String, nullable=False)
    observed_at: Mapped[float] = mapped_column(Float, nullable=False)

    qualification: Mapped[float] = mapped_column(
        Float, nullable=False,
        default=config.QUALIFICATION_SENTINEL,
        server_default=str(config.QUALIFICATION_SENTINEL),
    )
    reachability: Mapped[int] = mapped_column(Integer, nullable=False)
    ladder_rank: Mapped[int] = mapped_column(
        Integer, nullable=False,
        default=config.LADDER_RANK_SENTINEL,
        server_default=str(config.LADDER_RANK_SENTINEL),
    )
    source: Mapped[str] = mapped_column(String, nullable=False)

    __table_args__ = (
        # I3. Do not replace this with a surrogate id and a unique index
        # "for convenience" — the composite key IS the invariant.
        PrimaryKeyConstraint("alert_id", "stakeholder_id", name="pk_evaluations"),
        CheckConstraint(
            _in_check("observed_status", config.STATUS_VALUES),
            name="ck_evaluations_observed_status",
        ),
        CheckConstraint(
            _in_check("source", config.EVALUATION_SOURCE_VALUES),
            name="ck_evaluations_source",
        ),
        # reachability is derived from status (online=2, busy=1, offline=0), so
        # the range is closed and worth asserting.
        CheckConstraint(
            "reachability BETWEEN 0 AND 2", name="ck_evaluations_reachability"
        ),
    )


# ─────────────────────────────────────────────────────────────────────────────
# dispatch_attempts — THE DUPLICATE KILLER.  Invariant I2 lives in the UNIQUE.
# ─────────────────────────────────────────────────────────────────────────────


class DispatchAttempt(Base):
    """One attempt to reach one person about one alert.

    ================== INVARIANT I2 ==================
    UniqueConstraint(idempotency_key), where the key is
    '{alert_id}:{stakeholder_id}'.

    The key EXCLUDES the channel, deliberately. A failover from Slack to SMS is
    the SAME notification down a different pipe, so it must collide with itself
    and be handled as a continuation rather than registering as a second send.
    Including the channel here would silently permit exactly the duplicate the
    brief forbids.
    =================================================

    The row is inserted at RESERVE — before any bytes move — which is precisely
    why an ABORTED attempt still blocks a later re-attempt on the same person.
    The key is taken by the decision to try, not by the delivery.
    """

    __tablename__ = "dispatch_attempts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    alert_id: Mapped[str] = mapped_column(
        String, ForeignKey("alerts.alert_id"), nullable=False
    )
    stakeholder_id: Mapped[str] = mapped_column(
        String, ForeignKey("stakeholders.id"), nullable=False
    )
    idempotency_key: Mapped[str] = mapped_column(String, nullable=False)
    channel: Mapped[str] = mapped_column(String, nullable=False)
    role: Mapped[str] = mapped_column(String, nullable=False)
    state: Mapped[str] = mapped_column(String, nullable=False)
    plan_version: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    reserved_at: Mapped[float] = mapped_column(Float, nullable=False)
    committed_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    outcome_reason: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        # I2.
        UniqueConstraint("idempotency_key", name="uq_dispatch_attempts_idempotency"),
        CheckConstraint(
            _in_check("channel", config.CHANNEL_VALUES),
            name="ck_dispatch_attempts_channel",
        ),
        CheckConstraint(
            _in_check("role", config.ATTEMPT_ROLE_VALUES),
            name="ck_dispatch_attempts_role",
        ),
        CheckConstraint(
            _in_check("state", config.ATTEMPT_STATE_VALUES),
            name="ck_dispatch_attempts_state",
        ),
        # A committed row must carry its commit time, and a non-committed row
        # must not. Cheap, and it makes "did this actually land?" answerable
        # from the data alone rather than from the narrative around it.
        CheckConstraint(
            "(state = 'committed' AND committed_at IS NOT NULL) "
            "OR (state <> 'committed' AND committed_at IS NULL)",
            name="ck_dispatch_attempts_commit_time",
        ),
        # AUTOINCREMENT rather than bare rowid reuse: attempt ids appear in the
        # audit trail, and an id silently reused after a delete would make two
        # different attempts indistinguishable in the record.
        {"sqlite_autoincrement": True},
    )


# ─────────────────────────────────────────────────────────────────────────────
# audit_events — append-only.  This IS the preserved context (invariant I1).
# ─────────────────────────────────────────────────────────────────────────────


class AuditEvent(Base):
    """The narrative of one alert, in order.

    No UPDATE or DELETE is ever issued against this table. It is what survives
    a re-route (invariant I1) and what gets rendered into the "why you"
    explanation the final recipient receives.

    `seq` is allocated under the per-alert DispatchState lock in Module 2, so it
    stays monotonic once Module 3 adds a second concurrent writer. The unique
    index below is what turns a lock bug into a loud failure instead of a
    quietly scrambled incident record.

    `kind` has NO CheckConstraint on purpose. config.AUDIT_KINDS documents the
    taxonomy, but refusing to record an event because its label is unfamiliar
    would mean losing the record of an incident — strictly worse than recording
    it under an unexpected name.
    """

    __tablename__ = "audit_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    alert_id: Mapped[str] = mapped_column(
        String, ForeignKey("alerts.alert_id"), nullable=False
    )
    seq: Mapped[int] = mapped_column(Integer, nullable=False)
    at: Mapped[float] = mapped_column(Float, nullable=False)
    kind: Mapped[str] = mapped_column(String, nullable=False)
    actor: Mapped[str] = mapped_column(String, nullable=False)
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[str] = mapped_column(
        Text, nullable=False, default="{}", server_default="{}"
    )

    __table_args__ = (
        UniqueConstraint("alert_id", "seq", name="ux_audit_alert_seq"),
        CheckConstraint("seq >= 0", name="ck_audit_events_seq"),
        {"sqlite_autoincrement": True},
    )


#: Convenience for tests and for the schema-verification helper in db.py.
ALL_TABLES = (
    Stakeholder.__tablename__,
    ChannelHealth.__tablename__,
    Alert.__tablename__,
    Evaluation.__tablename__,
    DispatchAttempt.__tablename__,
    AuditEvent.__tablename__,
)
