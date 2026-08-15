"""
Async engine, connection pragmas, session factory, schema creation and seeding.

THE ONE THING TO GET RIGHT IN THIS FILE
---------------------------------------
`PRAGMA foreign_keys` is a PER-CONNECTION setting, not a per-database one.
SQLite ships with foreign keys OFF by default and ignores them silently, so a
pragma executed once inside init_db() protects exactly one connection and none
of the others the pool opens afterwards. The foreign keys declared in
models_orm.py would then be decorative, and — worse — the failure is invisible:
everything appears to work until an orphaned row shows up in the audit trail.

The fix is to register the pragmas as a `connect` event listener on the engine,
so they fire every single time the pool hands out a new DBAPI connection. That
registration happens inside build_engine(), which means TEST engines get it too.
That matters: if only the production engine enforced foreign keys, the test
asserting they are enforced would be testing a different object than the one
that runs.
"""

from __future__ import annotations

import json
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator, Callable

from sqlalchemy import event, insert, select, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from . import config
from .models_orm import Base, ChannelHealth, Stakeholder

Clock = Callable[[], float]


# ─────────────────────────────────────────────────────────────────────────────
# Engine construction
# ─────────────────────────────────────────────────────────────────────────────


def _register_pragmas(engine: AsyncEngine) -> None:
    """Attach the per-connection pragma listener.

    Called by build_engine() for EVERY engine, production or test. Listening on
    `engine.sync_engine` is the documented way to reach the DBAPI connect event
    from an async engine — the adapter exposes a synchronous cursor for exactly
    this purpose.
    """

    @event.listens_for(engine.sync_engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, _connection_record) -> None:  # noqa: ANN001
        cursor = dbapi_connection.cursor()
        try:
            # Without this the FKs in models_orm.py are ignored, silently.
            cursor.execute("PRAGMA foreign_keys=ON")

            # WAL lets readers proceed during a write. Module 3 runs the
            # executor and the interrupt listener concurrently against this
            # database, and without WAL they serialise into lock contention.
            # Not supported for in-memory databases, which simply report back
            # 'memory' rather than erroring — harmless either way.
            cursor.execute(f"PRAGMA journal_mode={config.SQLITE_JOURNAL_MODE}")

            # How long a writer waits for a competing writer before raising
            # "database is locked". Prevents intermittent Module 3 failures
            # that look like logic bugs and are really contention.
            cursor.execute(f"PRAGMA busy_timeout={config.SQLITE_BUSY_TIMEOUT_MS}")
        finally:
            cursor.close()


def build_engine(
    db_url: str | None = None,
    *,
    echo: bool = False,
    **engine_kwargs,
) -> AsyncEngine:
    """Create an async engine with the pragma listener already attached.

    Tests pass their own `db_url` (a tmp_path file) or their own pooling
    arguments. They must construct engines through THIS function rather than
    calling create_async_engine directly, or they lose foreign-key enforcement
    and end up proving something weaker than they claim.
    """
    engine = create_async_engine(db_url or config.DB_URL, echo=echo, **engine_kwargs)
    _register_pragmas(engine)
    return engine


def build_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Session factory for an engine.

    `expire_on_commit=False` because this system hands ORM data to frozen
    Pydantic models. With expiry on, touching an attribute after commit would
    trigger a refresh — a silent extra read, and in the registry's case a
    snapshot that starts tracking the database instead of staying a
    point-in-time observation.
    """
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


# ─────────────────────────────────────────────────────────────────────────────
# Module-level default engine (used by the CLI; tests build their own)
# ─────────────────────────────────────────────────────────────────────────────

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    """The process-wide default engine, created lazily on first use."""
    global _engine
    if _engine is None:
        _engine = build_engine()
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _session_factory
    if _session_factory is None:
        _session_factory = build_session_factory(get_engine())
    return _session_factory


@asynccontextmanager
async def session_scope() -> AsyncIterator[AsyncSession]:
    """A session from the default factory, closed on exit.

    Module 3 runs concurrent tasks. An AsyncSession is NOT concurrency-safe, so
    each task takes its own session from the factory — never a shared one.
    """
    factory = get_session_factory()
    async with factory() as session:
        yield session


async def dispose_engine() -> None:
    """Release pooled connections. Call at CLI shutdown and in test teardown."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _session_factory = None


# ─────────────────────────────────────────────────────────────────────────────
# Schema creation and seeding
# ─────────────────────────────────────────────────────────────────────────────


def _load_seed(seed_path: Path) -> list[dict]:
    if not seed_path.exists():
        raise FileNotFoundError(
            f"registry seed not found at {seed_path}. "
            "data/seed_stakeholders.json is required to initialise the database."
        )
    payload = json.loads(seed_path.read_text(encoding="utf-8"))
    records = payload.get("stakeholders")
    if not isinstance(records, list) or not records:
        raise ValueError(f"{seed_path} contains no 'stakeholders' array")
    return records


def _stakeholder_rows(records: list[dict], now: float) -> list[dict]:
    """Convert seed records into stakeholder INSERT parameters.

    JSON arrays are stored as TEXT so the registry's domain query can use
    SQLite's json_each() over them. Serialising here — rather than at query
    time — keeps that SQL readable.
    """
    rows: list[dict] = []
    for record in records:
        rows.append(
            {
                "id": record["id"],
                "name": record["name"],
                "title": record.get("title", ""),
                "primary_domain": record["primary_domain"],
                "secondary_domains": json.dumps(record.get("secondary_domains", [])),
                "seniority_tier": record["seniority_tier"],
                "preferred_channel": record["preferred_channel"],
                "fallback_channels": json.dumps(record.get("fallback_channels", [])),
                "status": record["status"],
                # SQLite has no boolean; the CHECK constraint pins this to 0/1.
                "on_call": int(bool(record.get("on_call", False))),
                "updated_at": now,
            }
        )
    return rows


def _channel_health_rows(records: list[dict], now: float) -> list[dict]:
    """One healthy row per (person, channel) in that person's channel_order.

    Seeding these up front means Module 3's "is a healthy fallback available?"
    is a plain SELECT. The alternative — treating a MISSING row as implicitly
    healthy — spreads a special case through every call site and makes
    "unknown" and "fine" indistinguishable.
    """
    rows: list[dict] = []
    for record in records:
        channels = [record["preferred_channel"], *record.get("fallback_channels", [])]
        seen: set[str] = set()
        for channel in channels:
            if channel in seen:
                continue
            seen.add(channel)
            rows.append(
                {
                    "stakeholder_id": record["id"],
                    "channel": channel,
                    "healthy": 1,
                    "last_error": None,
                    "updated_at": now,
                }
            )
    return rows


async def init_db(
    engine: AsyncEngine | None = None,
    *,
    seed_path: Path | None = None,
    clock: Clock = time.time,
    force_reseed: bool = False,
) -> None:
    """Create the schema and seed the registry.

    Idempotent: safe to call repeatedly. Seeding only runs when the
    stakeholders table is empty, unless `force_reseed=True`.

    NOTE FOR MODULES 2-6: because seeding is skipped on a non-empty database,
    editing data/seed_stakeholders.json has NO effect until the database is
    removed. Use `rm -f alert_router.db*` — the glob matters, since WAL leaves
    `-wal` and `-shm` sidecar files that preserve state on their own.
    """
    engine = engine or get_engine()
    seed_path = seed_path or config.SEED_PATH

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    factory = build_session_factory(engine)
    async with factory() as session:
        existing = await session.scalar(select(Stakeholder.id).limit(1))
        if existing is not None and not force_reseed:
            return

        if force_reseed:
            # Ordered child-first: foreign keys are enforced, so channel_health
            # cannot outlive the stakeholders it references.
            await session.execute(text("DELETE FROM channel_health"))
            await session.execute(text("DELETE FROM stakeholders"))

        records = _load_seed(seed_path)
        now = clock()
        await session.execute(insert(Stakeholder), _stakeholder_rows(records, now))
        await session.execute(
            insert(ChannelHealth), _channel_health_rows(records, now)
        )
        await session.commit()


async def reset_db(engine: AsyncEngine | None = None) -> None:
    """Drop and recreate everything. Test and demo affordance only."""
    engine = engine or get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)


# ─────────────────────────────────────────────────────────────────────────────
# Introspection helpers — for the review step and for tests
# ─────────────────────────────────────────────────────────────────────────────


async def foreign_keys_enabled(session: AsyncSession) -> bool:
    """Whether THIS session's connection enforces foreign keys.

    Exists so a test can assert the pragma on a second, freshly checked-out
    connection. Asserting it only on the connection that ran init_db() would
    pass even with the broken one-shot implementation, which is precisely the
    bug worth catching.
    """
    return bool(await session.scalar(text("SELECT * FROM pragma_foreign_keys")))


async def dump_schema(engine: AsyncEngine | None = None) -> dict[str, str]:
    """Table name -> the CREATE TABLE statement SQLite actually stored.

    Used by test_check_constraints_present_in_schema and by the manual review
    step. If a CHECK is absent from this output, create_all() did not emit it
    and the constraint is documentation rather than enforcement.
    """
    engine = engine or get_engine()
    async with engine.connect() as conn:
        result = await conn.execute(
            text(
                "SELECT name, sql FROM sqlite_master "
                "WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            )
        )
        return {row.name: row.sql for row in result}
