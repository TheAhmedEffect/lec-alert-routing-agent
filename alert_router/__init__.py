"""
Alert routing agent — LEC AI build assessment.

An agent that routes operational alerts to the right stakeholder, and handles
the case where availability changes WHILE the notification is being sent.

The four invariants, and where each one actually lives:

  I1  Context is never lost across a re-route
      -> frozen Pydantic models (schemas.py) + append-only audit_events
  I2  Nobody is notified twice
      -> UNIQUE(dispatch_attempts.idempotency_key) in models_orm.py
  I3  One availability query per person per alert
      -> PRIMARY KEY(evaluations.alert_id, stakeholder_id) in models_orm.py
  I4  Never escalate downward
      -> qualification carries no availability term (ranking.py, Module 2)

Two of those are database constraints rather than code paths. A second
availability query is not a bug this system tries to avoid; it is a write the
database refuses.

Module 1 surface only. Later modules add ranking, state, executor, decisions,
context, api and cli.
"""

from .config import (
    DB_URL,
    DOMAIN_POINTS,
    DOWNGRADE_TOLERANCE,
    LATENCY_MS_RANGE,
    MIN_QUALIFICATION,
    ON_CALL_POINTS,
    RNG_SEED,
    SENIORITY_POINTS,
)
from .db import (
    build_engine,
    build_session_factory,
    dispose_engine,
    foreign_keys_enabled,
    get_engine,
    get_session_factory,
    init_db,
    reset_db,
    session_scope,
)
from .ranking import (
    best_by_qualification,
    build_ladder,
    clears_floor,
    floor_for,
    score,
    sort_key,
)
from .registry import (
    DuplicateQueryError,
    PresenceBus,
    Registry,
    Subscription,
    UnknownStakeholder,
    default_latency,
    is_evaluations_duplicate,
    zero_latency,
)
from .state import DispatchState, persist_ladder
from .schemas import (
    AlertEvent,
    AttemptRecord,
    AttemptState,
    AuditEvent,
    Availability,
    CandidateSnapshot,
    Channel,
    DispatchPlan,
    InterruptEvent,
    InterruptKind,
    RankedCandidate,
    ScoreBreakdown,
    Severity,
    StakeholderRecord,
)

__all__ = [
    # config
    "DB_URL",
    "LATENCY_MS_RANGE",
    "RNG_SEED",
    "DOMAIN_POINTS",
    "SENIORITY_POINTS",
    "ON_CALL_POINTS",
    "MIN_QUALIFICATION",
    "DOWNGRADE_TOLERANCE",
    # ranking and state
    "score",
    "sort_key",
    "build_ladder",
    "clears_floor",
    "floor_for",
    "best_by_qualification",
    "DispatchState",
    "persist_ladder",
    # database
    "build_engine",
    "build_session_factory",
    "dispose_engine",
    "foreign_keys_enabled",
    "get_engine",
    "get_session_factory",
    "init_db",
    "reset_db",
    "session_scope",
    # registry
    "Registry",
    "PresenceBus",
    "Subscription",
    "DuplicateQueryError",
    "UnknownStakeholder",
    "is_evaluations_duplicate",
    "default_latency",
    "zero_latency",
    # contracts
    "AlertEvent",
    "AttemptRecord",
    "AttemptState",
    "AuditEvent",
    "Availability",
    "CandidateSnapshot",
    "Channel",
    "DispatchPlan",
    "InterruptEvent",
    "InterruptKind",
    "RankedCandidate",
    "ScoreBreakdown",
    "Severity",
    "StakeholderRecord",
]
