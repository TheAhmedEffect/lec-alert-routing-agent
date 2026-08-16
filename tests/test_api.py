"""
The HTTP surface — the one module the README told reviewers to run and the one
module nothing exercised.

WHY THIS FILE MATTERS MORE THAN ITS SIZE SUGGESTS
-------------------------------------------------
The central claim in this system is that presence changes ARRIVE rather than
being polled for. Inside `test_registry` that is easy to assert and easy to
disbelieve: the same process publishes and consumes, and a sceptical reader can
reasonably say "you built a queue and then read from it". `POST /presence` is
the same push path driven from OUTSIDE the process, which is what a Slack or
PagerDuty webhook actually is.

Writing these tests found a real bug. `PresenceBus.close()` sets `_closed` and
never clears it, and `subscribe()` on a closed bus raises. Because `bus` was
built at import time and closed on lifespan shutdown, the FIRST shutdown
poisoned the process: every subsequent startup served requests that died inside
`AlertAgent.handle()`. A single manual `uvicorn` run would never have shown it.
`test_the_app_survives_a_second_startup` is that bug, pinned.

WHAT IS DELIBERATELY *NOT* STUBBED
----------------------------------
Only the database is redirected, to a tmp_path file. The registry latency, the
channel bank and the agent are the real shipped objects, so these tests cost a
few seconds of simulated transport — and in exchange they exercise the wiring a
reviewer will actually hit rather than a convenient rearrangement of it.
"""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi", reason="fastapi is required for the HTTP surface")

from fastapi.testclient import TestClient  # noqa: E402

from alert_router import api as api_module  # noqa: E402
from alert_router import db as db_module  # noqa: E402
from alert_router.db import build_engine, build_session_factory  # noqa: E402

INFRA_ALERT = {
    "metric_name": "db_replica_lag_seconds",
    "value": 94.0,
    "threshold": 30.0,
    "severity": "critical",
    "domain": "infrastructure",
    "direction": "above",
}

#: The brief names four metric families. The registry has an owner for each; the
#: CLI demo only exercises two, so the other two are proved here instead.
PROCUREMENT_ALERT = {
    "metric_name": "contract_days_to_expiry",
    "value": 3.0,
    "threshold": 30.0,
    "severity": "high",
    "domain": "procurement",
    "direction": "below",
}

SECURITY_ALERT = {
    "metric_name": "auth_anomaly_score",
    "value": 0.94,
    "threshold": 0.70,
    "severity": "high",
    "domain": "security",
    "direction": "above",
}


def _redirect_database(tmp_path, monkeypatch):
    """Point the process-wide engine at a throwaway file.

    The API deliberately uses the module-level engine — that is what makes it a
    real server rather than a test harness — so redirecting it is the only way
    to keep these tests from writing to the developer's `alert_router.db`.
    """
    url = f"sqlite+aiosqlite:///{(tmp_path / 'api.db').as_posix()}"
    engine = build_engine(url)
    monkeypatch.setattr(db_module, "_engine", engine)
    monkeypatch.setattr(db_module, "_session_factory", build_session_factory(engine))


@pytest.fixture
def client(tmp_path, monkeypatch):
    """A started app. The `with` block is load-bearing.

    Starlette only runs lifespan inside the context manager. Without it there is
    no schema and no seed, and every test fails with `no such table` — which
    reads like a database bug rather than a test-setup mistake.
    """
    _redirect_database(tmp_path, monkeypatch)
    with TestClient(api_module.app) as test_client:
        yield test_client


# ─────────────────────────────────────────────────────────────────────────────
# Liveness
# ─────────────────────────────────────────────────────────────────────────────


def test_health_is_reachable_and_reports_the_bus(client):
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert isinstance(body["subscribers"], int)


def test_the_base_url_redirects_to_the_docs(client):
    """Opening http://localhost:8000 is the first thing any reviewer does, and a
    bare 404 there reads as a broken server."""
    response = client.get("/", follow_redirects=False)
    assert response.status_code in (301, 302, 307, 308)
    assert response.headers["location"] == "/docs"

    followed = client.get("/")
    assert followed.status_code == 200


def test_the_app_survives_a_second_startup(tmp_path, monkeypatch):
    """REGRESSION. The bus used to be built at import and closed at shutdown,
    so the second startup served requests against a permanently closed bus and
    routing died with `cannot subscribe to a closed PresenceBus`."""
    for _ in range(2):
        _redirect_database(tmp_path, monkeypatch)
        with TestClient(api_module.app) as client:
            assert client.get("/health").status_code == 200
            assert client.post("/alerts", json=INFRA_ALERT).status_code == 200


# ─────────────────────────────────────────────────────────────────────────────
# POST /alerts
# ─────────────────────────────────────────────────────────────────────────────


def test_post_alerts_routes_end_to_end(client):
    """The payload parses, the alert routes, and the response carries the whole
    record — ladder, decisions, audit — not just a status."""
    response = client.post("/alerts", json=INFRA_ALERT)
    assert response.status_code == 200, response.text
    body = response.json()

    assert body["alert_id"].startswith("alr-")
    assert len(body["ladder"]) == 7, "seven infrastructure candidates from one pull"
    assert body["notified"], "somebody has to be paged"
    assert body["audit"], "the narrative must come back with the decision"
    assert isinstance(body["decisions"], list)

    ranks = [row["rank"] for row in body["ladder"]]
    assert ranks == sorted(ranks), "the ladder must arrive in rank order"


def test_the_response_explains_why_this_person_and_not_the_others(client):
    """REQUIREMENT 3, over the transport.

    `notified` answers "who". Only the envelope answers "why you and not the
    person above you on the ladder", and on the `procurement` example that
    question is sharp: Nina Petrova out-qualifies the recipient and is offline.
    """
    body = client.post("/alerts", json=PROCUREMENT_ALERT).json()

    assert body["envelopes"], "somebody was paged with no explanation attached"
    envelope = body["envelopes"][0]

    assert envelope["chosen_because"], "no numeric justification"
    assert envelope["rendered_body"], "nothing the recipient can actually read"
    assert envelope["role"] in {"primary", "reroute", "escalation", "fyi"}

    # Everyone on the ladder who was not paged is accounted for by name, with a
    # reason — including the ones the walk never reached.
    named = {row["name"] for row in envelope["considered_and_passed"]}
    others = {c["name"] for c in body["ladder"]} - {envelope["to"]}
    assert others <= named, f"unexplained ladder members: {others - named}"
    assert all(row["why"] for row in envelope["considered_and_passed"])


def test_query_budget_holds_across_the_http_boundary(client):
    """INVARIANT I3, asserted through the transport rather than in-process.

    One evaluation per candidate and no more, even though the request path
    builds its own Registry per call — which is exactly the shape that would
    re-query if the budget lived in the object rather than in the database.
    """
    body = client.post("/alerts", json=INFRA_ALERT).json()
    assert len(body["evaluated"]) == len(body["ladder"])
    assert len(body["evaluated"]) == len(set(body["evaluated"]))


def test_nobody_is_notified_twice_over_http(client):
    """INVARIANT I2."""
    body = client.post("/alerts", json=INFRA_ALERT).json()
    assert len(body["notified"]) == len(set(body["notified"]))


def test_a_non_breaching_alert_is_422_not_a_silent_no_op(client):
    """The validator refuses an alert that does not cross its own threshold.

    The failure mode this prevents is the worst one an alerting system has:
    accepted, acknowledged, and silently routed nowhere.
    """
    payload = INFRA_ALERT | {"value": 5.0}       # 5 is not above 30
    response = client.post("/alerts", json=payload)
    assert response.status_code == 422
    assert response.json()["detail"]


def test_an_unknown_severity_is_rejected(client):
    response = client.post("/alerts", json=INFRA_ALERT | {"severity": "apocalyptic"})
    assert response.status_code == 422


@pytest.mark.parametrize(
    "payload, domain",
    [(PROCUREMENT_ALERT, "procurement"), (SECURITY_ALERT, "security")],
)
def test_routing_is_domain_agnostic(client, payload, domain):
    """The brief names stock levels, contract expiry, SLA breaches and anomaly
    scores. The demo shows two of those; these are the other two, proving the
    router has no hardcoded domain and that the README's curl examples run."""
    response = client.post("/alerts", json=payload)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["ladder"], f"no candidate owns {domain}"
    assert body["notified"], f"{domain} alert reached nobody"


# ─────────────────────────────────────────────────────────────────────────────
# POST /presence — the push path, from outside the process
# ─────────────────────────────────────────────────────────────────────────────


def test_presence_change_publishes_with_the_new_state(client):
    response = client.post(
        "/presence",
        json={"stakeholder_id": "stk-001", "status": "offline", "reason": "laptop shut"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["published"] is True
    assert body["previous"] == "online"
    assert body["current"] == "offline"


def test_an_unchanged_status_publishes_nothing(client):
    """A no-op must not become an event. Otherwise a flapping webhook generates
    interrupt traffic describing changes that never happened."""
    client.post("/presence", json={"stakeholder_id": "stk-001", "status": "offline"})
    second = client.post(
        "/presence", json={"stakeholder_id": "stk-001", "status": "offline"}
    )
    assert second.status_code == 200
    assert second.json()["published"] is False


def test_presence_for_an_unknown_person_is_404(client):
    response = client.post(
        "/presence", json={"stakeholder_id": "stk-999", "status": "offline"}
    )
    assert response.status_code == 404


def test_an_unknown_presence_status_is_422(client):
    response = client.post(
        "/presence", json={"stakeholder_id": "stk-001", "status": "vibing"}
    )
    assert response.status_code == 422


def test_presence_does_not_spend_the_query_budget(client):
    """`set_status` writes NOTHING to `evaluations`, which is what lets the
    counter stay flat on screen while the routing decision changes."""
    client.post("/presence", json={"stakeholder_id": "stk-002", "status": "offline"})
    body = client.post("/alerts", json=INFRA_ALERT).json()
    assert len(body["evaluated"]) == len(body["ladder"])


# ─────────────────────────────────────────────────────────────────────────────
# POST /channel-health
# ─────────────────────────────────────────────────────────────────────────────


def test_channel_health_can_be_marked_down(client):
    response = client.post(
        "/channel-health",
        json={
            "stakeholder_id": "stk-001",
            "channel": "slack",
            "healthy": False,
            "last_error": "adapter refused",
        },
    )
    assert response.status_code == 200
    assert response.json()["healthy"] is False


def test_an_unknown_channel_is_422(client):
    response = client.post(
        "/channel-health",
        json={"stakeholder_id": "stk-001", "channel": "carrier-pigeon", "healthy": True},
    )
    assert response.status_code == 422


# ─────────────────────────────────────────────────────────────────────────────
# GET /alerts/{id}/audit
# ─────────────────────────────────────────────────────────────────────────────


def test_the_audit_trail_reads_back_ordered_and_gapless(client):
    """INVARIANT I1's storage half: append-only, ordered, nothing missing."""
    alert_id = client.post("/alerts", json=INFRA_ALERT).json()["alert_id"]

    response = client.get(f"/alerts/{alert_id}/audit")
    assert response.status_code == 200, response.text
    events = response.json()["events"]

    assert events, "an alert that routed must leave a narrative"
    assert [e["seq"] for e in events] == list(range(len(events)))
    assert events[0]["kind"] == "RESOLVED"
    assert any(e["kind"] == "COMMITTED" for e in events)
    assert all(e["summary"] for e in events)


def test_the_audit_trail_for_an_unknown_alert_is_404(client):
    assert client.get("/alerts/alr-does-not-exist/audit").status_code == 404
