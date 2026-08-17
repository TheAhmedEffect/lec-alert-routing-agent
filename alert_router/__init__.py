

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

# ── Module 3 surface ─────────────────────────────────────────────────────────
from .agent import AlertAgent  # noqa: E402
from .channels import (  # noqa: E402
    ChannelAdapter,
    ChannelBank,
    ChannelConnectError,
    ChannelError,
    ChannelSendError,
    first_healthy_channel,
    first_persistent_channel,
    healthy_channels,
)
from .executor import (  # noqa: E402
    DispatchExecutor,
    DuplicateDispatchError,
    IllegalTransition,
    LEGAL_TRANSITIONS,
    PhaseHooks,
)
from .interrupts import InterruptListener  # noqa: E402

__all__ += [
    "AlertAgent",
    "ChannelAdapter",
    "ChannelBank",
    "ChannelError",
    "ChannelConnectError",
    "ChannelSendError",
    "healthy_channels",
    "first_healthy_channel",
    "first_persistent_channel",
    "DispatchExecutor",
    "DuplicateDispatchError",
    "IllegalTransition",
    "LEGAL_TRANSITIONS",
    "PhaseHooks",
    "InterruptListener",
]

# ── Module 4 surface ─────────────────────────────────────────────────────────
from .decisions import (  # noqa: E402
    MATRIX,
    ROW_IDS,
    ChannelFacts,
    NoMatchingRow,
    decide,
)

__all__ += ["MATRIX", "ROW_IDS", "ChannelFacts", "NoMatchingRow", "decide"]

# ── Module 5 surface ─────────────────────────────────────────────────────────
from .context import (  # noqa: E402
    compile_envelope,
    deliver_envelope,
    render,
    render_template,
)

__all__ += ["compile_envelope", "deliver_envelope", "render", "render_template"]

# ── Module 6 surface ─────────────────────────────────────────────────────────
from .scenarios import (  # noqa: E402
    SCENARIO_NAMES,
    ScenarioResult,
    run_all,
    run_scenario,
)

__all__ += ["SCENARIO_NAMES", "ScenarioResult", "run_scenario", "run_all"]
