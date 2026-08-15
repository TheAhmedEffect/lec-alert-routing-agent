"""
Every tunable number and every enumerated domain value in the system.

WHY THIS FILE EXISTS
--------------------
Two reasons, and the second one matters more than it looks.

1. A reviewer will want to change a weight and re-run. If `DOWNGRADE_TOLERANCE`
   is buried in three different modules, that is a code change rather than a
   configuration change, and the system stops being explainable.

2. The enumerated value tuples below (CHANNEL_VALUES, STATUS_VALUES, ...) are
   the SINGLE source of truth shared by the database CHECK constraints in
   models_orm.py and the Pydantic enums in schemas.py. If those two drifted
   apart, the database would accept a value the application cannot represent,
   or the application would emit a value the database rejects at 3am. Deriving
   both from one tuple makes that class of bug unrepresentable rather than
   merely unlikely.

Nothing in this file imports from the rest of the package, so it can never
participate in a circular import.
"""

from __future__ import annotations

from pathlib import Path
from typing import Final

# ─────────────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────────────

PACKAGE_ROOT: Final[Path] = Path(__file__).resolve().parent
PROJECT_ROOT: Final[Path] = PACKAGE_ROOT.parent

#: The registry seed. Loaded once by init_db() on a fresh database.
SEED_PATH: Final[Path] = PROJECT_ROOT / "data" / "seed_stakeholders.json"

#: On-disk database. A FILE rather than :memory: on purpose — the ability to
#: open it in `sqlite3` and read `.schema` by hand is how we prove the
#: invariants are structural rather than documented.
DB_FILENAME: Final[str] = "alert_router.db"
DB_PATH: Final[Path] = PROJECT_ROOT / DB_FILENAME

#: NOTE THE as_posix(). On Windows, str(Path) yields backslashes
#: (C:\Users\...\alert_router.db) and embedding those in a URL is at best
#: ambiguous and at worst mangled by URL parsing. Forward slashes are accepted
#: by SQLite on every platform, so as_posix() is portable rather than merely
#: tidy — and this project is being built on Windows.
DB_URL: Final[str] = f"sqlite+aiosqlite:///{DB_PATH.as_posix()}"

# ─────────────────────────────────────────────────────────────────────────────
# SQLite connection pragmas
# ─────────────────────────────────────────────────────────────────────────────
# These are applied by a `connect` event listener in db.py, NOT once at startup.
# PRAGMA settings are per-connection: setting foreign_keys on a single
# connection leaves every other pooled connection unenforced, silently, and the
# foreign keys in models_orm.py become decorative.

#: Milliseconds a writer waits for a competing writer before raising
#: "database is locked". Module 3 runs the executor and the interrupt listener
#: concurrently, so without this the suite fails intermittently under load.
SQLITE_BUSY_TIMEOUT_MS: Final[int] = 5_000

#: Write-Ahead Logging lets readers proceed during a write. Also needed by
#: Module 3. Note that WAL leaves `-wal` and `-shm` sidecar files next to the
#: database — `rm alert_router.db` alone does NOT reset state; use the glob.
SQLITE_JOURNAL_MODE: Final[str] = "WAL"

# ─────────────────────────────────────────────────────────────────────────────
# Registry pull simulation
# ─────────────────────────────────────────────────────────────────────────────

#: A registry lookup is I/O. Making that cost real is not decoration: it is what
#: creates the window in which the world can change underneath an in-flight
#: dispatch, which is the entire scenario this assessment is about. With zero
#: latency there is no mid-flight.
LATENCY_MS_RANGE: Final[tuple[int, int]] = (120, 300)

#: Seeded so a recorded demo is reproducible. A walkthrough where the timings
#: differ on every take is very hard to narrate.
RNG_SEED: Final[int] = 7

# ─────────────────────────────────────────────────────────────────────────────
# Query-ledger sentinels
# ─────────────────────────────────────────────────────────────────────────────
# `evaluations` is written at PULL time, but `qualification` and `ladder_rank`
# are products of RANKING, which is Module 2. Rather than let the registry
# import the scoring function — which would collapse the layer split and make
# ranking untestable without a database — the pull writes sentinels and
# Module 2's persist_ladder() updates them.
#
# LADDER_RANK_SENTINEL is -1 and not 0 precisely so it can never be misread as
# a legitimate rank.

QUALIFICATION_SENTINEL: Final[float] = 0.0
LADDER_RANK_SENTINEL: Final[int] = -1

# ─────────────────────────────────────────────────────────────────────────────
# Enumerated domain values — shared by CHECK constraints and Pydantic enums
# ─────────────────────────────────────────────────────────────────────────────

#: Notification transports. Slack is synchronous; email and SMS are persistent
#: (they wait on the device). That distinction is a first-class property in
#: schemas.py because it is why a presence drop mid-SMS is a NON-EVENT.
CHANNEL_VALUES: Final[tuple[str, ...]] = ("slack", "email", "sms")

#: Human presence. Deliberately three-valued: "busy" is reachable-but-costly,
#: and collapsing it into online/offline loses the ability to prefer someone
#: who is free over someone who is merely present.
STATUS_VALUES: Final[tuple[str, ...]] = ("online", "busy", "offline")

#: Breach direction. "below" exists because depletion metrics — stock levels,
#: contract expiry, remaining credit — breach by falling. A router that only
#: understands `value > threshold` drops those silently.
DIRECTION_VALUES: Final[tuple[str, ...]] = ("above", "below")

SEVERITY_VALUES: Final[tuple[str, ...]] = ("low", "high", "critical")

ALERT_STATE_VALUES: Final[tuple[str, ...]] = (
    "routing",
    "delivered",
    "escalated",
    "exhausted",
)

#: The dispatch state machine. `reserved` exists before anything is sent, which
#: is what makes an ABORTED attempt still block a re-attempt: the idempotency
#: key is taken at reservation, not at delivery.
ATTEMPT_STATE_VALUES: Final[tuple[str, ...]] = (
    "reserved",
    "connecting",
    "in_flight",
    "committed",
    "aborted",
    "failed",
)

ATTEMPT_ROLE_VALUES: Final[tuple[str, ...]] = (
    "primary",
    "reroute",
    "escalation",
    "fyi",
)

#: How a fact about a person was learned. Every row this system writes is
#: 'pull'; see the note on push in registry.py.
EVALUATION_SOURCE_VALUES: Final[tuple[str, ...]] = ("pull", "push")

#: Audit event taxonomy. Not a CHECK constraint — the audit trail should never
#: refuse a write because a new event kind appeared. Losing the record of an
#: incident is strictly worse than recording it under an unexpected label.
AUDIT_KINDS: Final[tuple[str, ...]] = (
    "RESOLVED",
    "RANKED",
    "RESERVED",
    "INTERRUPT",
    "DECISION",
    "ABORTED",
    "COMMITTED",
    "SUPPRESSED",
    "EXHAUSTED",
)

# ─────────────────────────────────────────────────────────────────────────────
# Seniority bounds
# ─────────────────────────────────────────────────────────────────────────────
# Referenced by the CHECK constraint on stakeholders.seniority_tier and by the
# Pydantic Field(ge=..., le=...) in schemas.py, so the two cannot drift.

SENIORITY_TIER_MIN: Final[int] = 1
SENIORITY_TIER_MAX: Final[int] = 5

# ─────────────────────────────────────────────────────────────────────────────
# Module 2 will append here
# ─────────────────────────────────────────────────────────────────────────────
# DOMAIN_POINTS, SENIORITY_POINTS, ON_CALL_POINTS, MIN_QUALIFICATION and
# DOWNGRADE_TOLERANCE belong in this file and are deliberately NOT defined yet.
# Module 1 must not be able to score anyone: if the registry could compute a
# qualification, it would, and the pull/rank boundary would erode on the first
# convenient occasion.
