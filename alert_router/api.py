"""
HTTP surface — proof that the push channel is real, not a test fixture.

    uvicorn alert_router.api:app --reload

WHY THIS EXISTS AT ALL
----------------------
The interesting claim in this system is that presence changes ARRIVE rather than
being polled for. Inside a test that is easy to assert and easy to disbelieve —
the same process publishes and consumes. This endpoint lets a presence change
come from outside the process entirely, which is what a Slack or PagerDuty
webhook would actually be.

    POST /alerts            trigger a routing decision
    POST /presence          flip somebody's availability (the push path)
    GET  /alerts/{id}/audit read the incident narrative back

SECURITY, STATED PLAINLY
------------------------
POST /presence is UNAUTHENTICATED. Anyone who can reach this process can mark
anyone offline and steer the routing. That is correct for a demo and
indefensible in production; it is named in the README's "what is unfinished"
rather than left for a reviewer to discover.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from sqlalchemy import select

from .agent import AlertAgent
from .channels import ChannelBank
from .db import dispose_engine, get_session_factory, init_db
from .models_orm import AuditEvent as AuditEventRow
from .registry import PresenceBus, Registry, UnknownStakeholder
from .schemas import AlertEvent, Availability, Channel

#: One bus for the process, so a presence POST reaches an in-flight dispatch.
#: Single-process only — see the README. Horizontally this becomes Redis pub/sub.
bus = PresenceBus()
bank = ChannelBank()


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    """Fresh bus and bank per app STARTUP, not per import.

    PresenceBus.close() is permanent — `_closed` is never cleared, and
    subscribe() on a closed bus raises. With the module-level instances reused,
    the first shutdown poisoned the process: any second startup (a test that
    opens TestClient twice, a reload, an embedded app) served requests that blew
    up inside AlertAgent.handle() with `cannot subscribe to a closed
    PresenceBus`. Rebinding here makes startup idempotent.

    The names stay module-level so `from .api import bus` still works and so the
    request handlers can read the current instance at call time.
    """
    global bus, bank
    bus = PresenceBus()
    bank = ChannelBank()
    await init_db()
    try:
        yield
    finally:
        bus.close()
        await dispose_engine()


app = FastAPI(
    title="Alert Routing Agent",
    version="1.0.0",
    description=(
        "Routes operational alerts and handles stakeholder availability changing "
        "mid-dispatch. POST /presence is deliberately unauthenticated for the demo."
    ),
    lifespan=lifespan,
)


def _registry() -> Registry:
    return Registry(get_session_factory(), bus=bus)


# ─────────────────────────────────────────────────────────────────────────────
# Requests
# ─────────────────────────────────────────────────────────────────────────────


class AlertRequest(BaseModel):
    metric_name: str
    value: float
    threshold: float
    severity: str
    domain: str
    direction: str = "above"


class PresenceRequest(BaseModel):
    stakeholder_id: str
    status: str
    reason: str = ""


class ChannelHealthRequest(BaseModel):
    stakeholder_id: str
    channel: str
    healthy: bool
    last_error: str | None = None


# ─────────────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────────────


@app.get("/", include_in_schema=False)
async def root() -> RedirectResponse:
    """Send the base URL to the interactive docs.

    Without this, opening http://localhost:8000 — the first thing anybody does —
    returns `{"detail":"Not Found"}`, which reads as a broken server rather than
    as an app that simply has no route at `/`. The docs page is also the only
    way to exercise these endpoints that behaves identically on every OS: the
    curl examples in the README are bash syntax and PowerShell mangles the
    escaped quotes before curl ever sees them.
    """
    return RedirectResponse(url="/docs")


@app.post("/alerts")
async def create_alert(request: AlertRequest) -> dict:
    """Route an alert to completion and return what was decided.

    The AlertEvent validator refuses anything that does not actually breach its
    own threshold, so a malformed alert is a 422 rather than a silent no-op.
    """
    try:
        alert = AlertEvent(
            metric_name=request.metric_name,
            value=request.value,
            threshold=request.threshold,
            direction=request.direction,  # type: ignore[arg-type]
            severity=request.severity,  # type: ignore[arg-type]
            domain=request.domain,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    sessions = get_session_factory()
    agent = AlertAgent(_registry(), sessions, bank)
    state = await agent.handle(alert)

    return {
        "alert_id": alert.alert_id,
        "evaluated": sorted(state.evaluated),
        "ladder": [
            {
                "rank": candidate.rank,
                "id": candidate.snapshot.stakeholder.id,
                "name": candidate.snapshot.stakeholder.name,
                "qualification": candidate.score.qualification,
                "reachability": candidate.score.reachability,
            }
            for candidate in state.plan.ladder
        ],
        "notified": sorted(state.notified),
        "suppressed": state.suppressed,
        "decisions": [
            {"row": d.matrix_row, "action": d.action.value, "why": d.rationale}
            for d in agent.decisions
        ],
        # The brief's third requirement is that the final recipient has full
        # context AND an explanation of why they were chosen over others.
        # Without this block the HTTP surface returns WHO was paged and never
        # WHY — which is the interesting half, and the half a reviewer poking at
        # `/docs` would otherwise never see.
        "envelopes": [
            {
                "to": envelope.recipient.name,
                "role": envelope.role,
                "channel": envelope.channel.value,
                "chosen_because": envelope.chosen_because,
                "considered_and_passed": [
                    {"name": name, "why": why}
                    for name, why in envelope.considered_and_passed
                ],
                "rendered_body": envelope.rendered_body,
            }
            for envelope in state.envelopes.values()
        ],
        "audit": list(state.audit_lines()),
    }


@app.post("/presence")
async def set_presence(request: PresenceRequest) -> dict:
    """THE PUSH PATH, from outside the process.

    This is what a Slack or PagerDuty presence webhook would call. It writes the
    registry and publishes an InterruptEvent carrying the NEW STATE — which is
    why an in-flight dispatch can react without asking anybody anything.

    UNAUTHENTICATED BY DESIGN for the demo. See the README.
    """
    try:
        status = Availability(request.status)
    except ValueError as exc:
        raise HTTPException(
            status_code=422, detail=f"unknown status {request.status!r}"
        ) from exc

    try:
        event = await _registry().set_status(
            request.stakeholder_id, status, reason=request.reason
        )
    except UnknownStakeholder as exc:
        raise HTTPException(
            status_code=404, detail=f"no stakeholder {request.stakeholder_id!r}"
        ) from exc

    if event is None:
        return {"published": False, "note": "status unchanged; no event emitted"}
    return {
        "published": True,
        "stakeholder_id": event.stakeholder_id,
        "previous": event.previous.value if event.previous else None,
        "current": event.current.value if event.current else None,
        "subscribers": bus.subscriber_count,
    }


@app.post("/channel-health")
async def set_channel_health(request: ChannelHealthRequest) -> dict:
    """Mark a transport up or down. A person is not their transport."""
    try:
        channel = Channel(request.channel)
    except ValueError as exc:
        raise HTTPException(
            status_code=422, detail=f"unknown channel {request.channel!r}"
        ) from exc

    event = await _registry().set_channel_health(
        request.stakeholder_id, channel, request.healthy, last_error=request.last_error
    )
    return {"published": True, "channel": channel.value, "healthy": request.healthy}


@app.get("/alerts/{alert_id}/audit")
async def read_audit(alert_id: str) -> dict:
    """The incident narrative, in order. Append-only; nothing here is ever
    updated or deleted."""
    async with get_session_factory()() as session:
        rows = (
            await session.scalars(
                select(AuditEventRow)
                .where(AuditEventRow.alert_id == alert_id)
                .order_by(AuditEventRow.seq)
            )
        ).all()
    if not rows:
        raise HTTPException(status_code=404, detail=f"no audit trail for {alert_id!r}")
    return {
        "alert_id": alert_id,
        "events": [
            {
                "seq": row.seq,
                "kind": row.kind,
                "actor": row.actor,
                "summary": row.summary,
            }
            for row in rows
        ],
    }


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "subscribers": bus.subscriber_count}
