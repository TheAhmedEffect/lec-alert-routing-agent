"""
The final gate — all four invariants, checked against all four scenarios.

Every other test file checks a MECHANISM: the primary key, the shield, the
matrix row. This one checks the OUTCOME. Both matter, and they fail differently:
a mechanism test tells you which line broke, an outcome test tells you that
something broke at all even when every unit still passes.

The scenarios run end to end here — one real pull, a real dispatch, a real
interrupt, a real decision — so these assertions are about the system rather
than about any part of it.
"""

from __future__ import annotations

import pytest

from alert_router.scenarios import SCENARIO_NAMES, ScenarioResult, run_scenario


@pytest.fixture(scope="module")
def _scenario_cache() -> dict[str, ScenarioResult]:
    return {}


async def _result(name: str, tmp_path_factory, cache) -> ScenarioResult:
    """Run each scenario once and share it across the invariant checks.

    Four scenarios x four invariants would otherwise be sixteen full dispatches,
    which is slow enough that the suite stops being run.
    """
    if name not in cache:
        directory = tmp_path_factory.mktemp(f"scenario-{name}")
        cache[name] = await run_scenario(name, db_path=directory / f"{name}.db")
    return cache[name]


@pytest.mark.parametrize("name", SCENARIO_NAMES)
async def test_scenario_fires_its_expected_matrix_row(
    name, tmp_path_factory, _scenario_cache
):
    """Each scenario exists to demonstrate one row. If it stops firing that row,
    the scenario is no longer demonstrating what it claims."""
    result = await _result(name, tmp_path_factory, _scenario_cache)
    assert result.matched_expectation, (
        f"{name}: expected {result.expected_row}, fired {result.rows_fired}"
    )


@pytest.mark.parametrize("name", SCENARIO_NAMES)
async def test_i1_context_is_preserved(name, tmp_path_factory, _scenario_cache):
    """INVARIANT I1 — every envelope carries the ORIGINAL alert object.

    Identity, not equality: a faithful copy would satisfy `==` and would still
    be the bug this invariant exists to catch.
    """
    result = await _result(name, tmp_path_factory, _scenario_cache)
    state = result.state

    assert state.envelopes, f"{name}: nobody received an explanation"
    for envelope in state.envelopes.values():
        assert envelope.alert is state.alert
        assert envelope.audit_trail, "the envelope must carry the narrative"
        assert envelope.chosen_because, "and the reason this person was chosen"
        assert envelope.rendered_body, "and something the recipient can read"


@pytest.mark.parametrize("name", SCENARIO_NAMES)
async def test_i2_nobody_is_notified_twice(name, tmp_path_factory, _scenario_cache):
    """INVARIANT I2 — no duplicates, on any channel, under any interleaving.

    Checked three ways: the in-memory ledger, the committed attempt rows, and the
    idempotency keys. The keys are the real guarantee; the other two would still
    pass if the database constraint were quietly dropped, which is exactly why
    all three are here.
    """
    result = await _result(name, tmp_path_factory, _scenario_cache)
    state = result.state

    assert len(state.notified) == len(set(state.notified))

    committed = [
        person_id
        for person_id, record in state.attempted.items()
        if record.state.value == "committed"
    ]
    assert len(committed) == len(set(committed))
    assert set(committed) == set(state.notified)

    keys = [record.idempotency_key for record in state.attempted.values()]
    assert len(keys) == len(set(keys)), f"{name}: a duplicate idempotency key exists"

    # And the channel is genuinely excluded from the key, so a failover cannot
    # masquerade as a second notification.
    for person_id, record in state.attempted.items():
        assert record.idempotency_key == f"{state.alert.alert_id}:{person_id}"
        assert record.channel.value not in record.idempotency_key


@pytest.mark.parametrize("name", SCENARIO_NAMES)
async def test_i3_one_query_per_person(name, tmp_path_factory, _scenario_cache):
    """INVARIANT I3 — the query budget, measured against the ledger itself.

    The subtle assertion is the last one: everybody the agent ACTED on must have
    been someone it already paid to evaluate. Acting on a stranger would mean a
    pull happened somewhere off the books.
    """
    result = await _result(name, tmp_path_factory, _scenario_cache)
    state = result.state

    assert result.query_count == len(state.evaluated)
    assert result.query_count == len(set(state.evaluated))

    touched = set(state.attempted) | set(state.notified) | set(state.suppressed)
    assert touched <= state.evaluated, (
        f"{name}: acted on {touched - state.evaluated} without evaluating them"
    )


@pytest.mark.parametrize("name", SCENARIO_NAMES)
async def test_i4_never_escalates_downward(name, tmp_path_factory, _scenario_cache):
    """INVARIANT I4 — nobody notified is less qualified than somebody suppressed.

    This is the invariant asserted against the OUTCOME rather than the scoring
    function. test_ranking proves qualification ignores availability; this proves
    the system as a whole never acted as though it did not.
    """
    result = await _result(name, tmp_path_factory, _scenario_cache)
    state = result.state

    by_id = {c.snapshot.stakeholder.id: c for c in state.plan.ladder}
    notified = [by_id[p].score.qualification for p in state.notified if p in by_id]
    suppressed = [by_id[p].score.qualification for p in state.suppressed if p in by_id]

    if notified and suppressed:
        assert min(notified) > max(suppressed), (
            f"{name}: notified someone less qualified than a suppressed candidate"
        )

    # And qualification never moved because of availability, even after patching.
    for candidate in state.plan.ladder:
        score = candidate.score
        assert score.qualification == (
            score.domain_points + score.seniority_points + score.on_call_points
        ), "an availability term crept into qualification"


@pytest.mark.parametrize("name", SCENARIO_NAMES)
async def test_audit_trail_is_ordered_and_gapless(
    name, tmp_path_factory, _scenario_cache
):
    """The narrative is evidence. Evidence with holes in it is not evidence."""
    result = await _result(name, tmp_path_factory, _scenario_cache)
    events = sorted(result.state.audit, key=lambda e: e.seq)

    assert [e.seq for e in events] == list(range(len(events)))
    assert events[0].kind == "RESOLVED"
    assert any(e.kind == "COMMITTED" for e in events), f"{name}: nothing was delivered"
    assert all(e.summary for e in events), "an audit line with no summary"


async def test_the_floor_scenario_refuses_to_route_down(
    tmp_path_factory, _scenario_cache
):
    """The single most important behaviour in this submission, asserted directly.

    Everyone demonstrates a re-route. This is the one that refuses one — and
    records the arithmetic that justified refusing.
    """
    result = await _result("floor", tmp_path_factory, _scenario_cache)
    state = result.state

    assert "R4" in result.rows_fired
    assert "stk-002" in state.suppressed, "Tom should have been declined, not skipped"
    reason = state.suppressed["stk-002"]
    assert all(token in reason for token in ("108", "120", "139"))
    assert "stk-002" not in state.notified

    # The incumbent is kept, on a channel that survives them being offline.
    incumbent = state.attempted["stk-001"]
    assert incumbent.channel.is_persistent
    assert "stk-001" in state.notified
    # ...and somebody MORE qualified was paged in parallel.
    assert "stk-003" in state.notified


async def test_the_failover_scenario_keeps_one_attempt_row(
    tmp_path_factory, _scenario_cache
):
    """Row R6: the transport changed, the person did not, and the key never moved."""
    result = await _result("failover", tmp_path_factory, _scenario_cache)
    state = result.state

    assert "R6" in result.rows_fired
    assert len(state.notified) == 1
    person_id = next(iter(state.notified))
    record = state.attempted[person_id]
    assert not record.channel.is_persistent or record.channel.value != "slack"
    assert record.idempotency_key == f"{state.alert.alert_id}:{person_id}"

    # A depletion breach: it crossed the threshold by falling.
    assert state.alert.direction == "below"
    assert state.alert.value < state.alert.threshold
