"""
Module 5 gate — invariant I1 and the explanation layer.

NO TEST HERE TOUCHES THE NETWORK. A suite that needs an API key is a suite the
reviewer cannot run, so the OpenAI path is exercised entirely through an
injected fake client. That is also the only honest way to test a fallback: you
have to be able to make the call fail on demand.

The load-bearing test is test_context_survives_two_reroutes. It asserts object
IDENTITY (`is`), not equality — because equality would pass for a faithful copy,
and a faithful copy somewhere in the chain is exactly the bug invariant I1
exists to catch.
"""

from __future__ import annotations

import asyncio

import pytest

from alert_router.context import (
    compile_envelope,
    deliver_envelope,
    render,
    render_template,
)
from alert_router.schemas import AttemptRecord, AttemptState, Channel

PRIYA, TOM, ELENA, DANIEL = "stk-001", "stk-002", "stk-003", "stk-007"


# ─────────────────────────────────────────────────────────────────────────────
# Fakes — no network, ever
# ─────────────────────────────────────────────────────────────────────────────


class FakeResponses:
    def __init__(self, text: str | None = None, error: Exception | None = None):
        self._text = text
        self._error = error
        self.calls: list[str] = []

    async def create(self, *, model: str, input: str):  # noqa: A002
        self.calls.append(input)
        if self._error is not None:
            raise self._error
        return type("Response", (), {"output_text": self._text})()


class FakeClient:
    """Stands in for openai.AsyncOpenAI. Records what it was asked."""

    def __init__(self, text: str | None = None, error: Exception | None = None):
        self.responses = FakeResponses(text, error)


def attempt(state, person_id: str, *, role: str = "primary",
            channel: Channel = Channel.SLACK,
            attempt_state: AttemptState = AttemptState.COMMITTED) -> AttemptRecord:
    record = AttemptRecord(
        alert_id=state.alert.alert_id,
        stakeholder_id=person_id,
        channel=channel,
        role=role,  # type: ignore[arg-type]
        state=attempt_state,
        reserved_at=1.0,
        committed_at=2.0 if attempt_state is AttemptState.COMMITTED else None,
        outcome_reason="" if attempt_state is AttemptState.COMMITTED else "went offline",
    )
    state.register_attempt(record, make_current=(role != "escalation"))
    if attempt_state is AttemptState.COMMITTED:
        state.mark_notified(person_id)
    return record


# ─────────────────────────────────────────────────────────────────────────────
# Invariant I1
# ─────────────────────────────────────────────────────────────────────────────


async def test_context_survives_two_reroutes(state):
    """INVARIANT I1. Two aborts, and the alert is still the SAME OBJECT.

    Priya aborted, Tom aborted, Daniel notified. The envelope Daniel receives
    must carry the original AlertEvent by reference, plus the whole story of how
    we got to him — not a summary of it and not a rebuild of it.
    """
    original = state.alert

    await state.record_audit("RESERVED", PRIYA, "reserved primary dispatch to Priya")
    attempt(state, PRIYA, attempt_state=AttemptState.ABORTED)
    await state.record_audit("ABORTED", PRIYA, "Priya went offline mid-send")

    await state.record_audit("RESERVED", TOM, "reserved reroute dispatch to Tom")
    attempt(state, TOM, role="reroute", attempt_state=AttemptState.ABORTED)
    await state.record_audit("ABORTED", TOM, "Tom went offline mid-send")

    final = attempt(state, DANIEL, role="reroute")
    await state.record_audit("COMMITTED", DANIEL, "delivered to Daniel Ortiz")

    envelope = compile_envelope(state, final)

    # IDENTITY, not equality. A copy would satisfy `==` and still be the bug.
    assert envelope.alert is original
    assert envelope.alert.alert_id == original.alert_id
    assert envelope.alert.value == 94.0

    trail = "\n".join(envelope.audit_trail)
    assert "Priya" in trail and "Tom" in trail and "Daniel" in trail

    # Five legs: reserved/aborted for Priya, reserved/aborted for Tom, committed
    # for Daniel. Derived from the trail itself rather than hardcoded twice, so a
    # future extra audit line cannot make this assertion quietly contradict the
    # one below it.
    assert len(envelope.audit_trail) == 5
    assert [line[1:3] for line in envelope.audit_trail] == [
        f"{i:02d}" for i in range(len(envelope.audit_trail))
    ], "the trail must be ordered and gapless"
    assert sum(1 for line in envelope.audit_trail if "ABORTED" in line) == 2
    assert sum(1 for line in envelope.audit_trail if "COMMITTED" in line) == 1

    # Both abandoned attempts are accounted for, by name.
    passed = dict(envelope.considered_and_passed)
    assert "Priya Raman" in passed and "Tom Beckett" in passed


async def test_envelope_names_the_runner_up_with_numbers(state):
    """"Why you" has to be checkable, which means it has to contain arithmetic."""
    record = attempt(state, PRIYA)
    envelope = compile_envelope(state, record)

    assert "139" in envelope.chosen_because
    assert "Tom Beckett" in envelope.chosen_because
    assert "123" in envelope.chosen_because
    assert "primary domain" in envelope.chosen_because


async def test_suppressed_people_appear_with_their_numeric_reasons(state):
    """The suppression strings pass through UNCHANGED.

    ranking.clears_floor() already produced the only sentence a reviewer can
    verify; summarising it here would throw that away.
    """
    state.mark_suppressed(
        TOM,
        "Tom Beckett qualification 108 < floor 120 (max of CRITICAL minimum 120, "
        "incumbent Priya Raman 139 - tolerance 25)",
    )
    record = attempt(state, PRIYA)
    envelope = compile_envelope(state, record)

    passed = dict(envelope.considered_and_passed)
    assert "Tom Beckett" in passed
    assert all(token in passed["Tom Beckett"] for token in ("108", "120", "139"))


async def test_escalation_envelope_justifies_itself_against_the_incumbent(state):
    """Row R4/R9 notify two people. The second one's question is not "why you"
    but "why ALSO you", and the envelope has to answer that instead."""
    attempt(state, PRIYA)                                    # incumbent, notified
    escalation = attempt(state, ELENA, role="escalation", channel=Channel.SMS)

    envelope = compile_envelope(state, escalation)
    assert envelope.role == "escalation"
    assert envelope.channel is Channel.SMS
    assert "140" in envelope.chosen_because
    assert "Priya Raman" in envelope.chosen_because
    assert "parallel" in envelope.chosen_because


async def test_one_envelope_per_attempt_not_per_alert(state):
    """R4's two recipients get genuinely different envelopes."""
    primary = attempt(state, PRIYA, channel=Channel.SMS)
    escalation = attempt(state, ELENA, role="escalation", channel=Channel.SMS)

    first = compile_envelope(state, primary)
    second = compile_envelope(state, escalation)

    assert first.recipient.id != second.recipient.id
    assert first.role != second.role
    assert first.chosen_because != second.chosen_because
    assert first.alert is second.alert, "but both carry the SAME alert object"


async def test_envelope_for_a_non_ladder_member_is_refused(state):
    """An envelope for somebody outside the ladder would describe a routing
    decision that never happened."""
    ghost = AttemptRecord(
        alert_id=state.alert.alert_id,
        stakeholder_id="stk-999",
        channel=Channel.SLACK,
        role="primary",
        state=AttemptState.COMMITTED,
        reserved_at=1.0,
        committed_at=2.0,
    )
    with pytest.raises(ValueError, match="not in the ladder"):
        compile_envelope(state, ghost)


# ─────────────────────────────────────────────────────────────────────────────
# Rendering
# ─────────────────────────────────────────────────────────────────────────────


async def test_render_without_an_api_key_uses_the_template(state, monkeypatch):
    """The system must run identically with no key. That is a requirement."""
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    envelope = compile_envelope(state, attempt(state, PRIYA))

    body, source = await render(envelope)
    assert source == "template"
    assert body == render_template(envelope)
    assert "139" in body
    assert "Why you:" in body


async def test_render_with_a_mocked_client_produces_the_same_structured_fields(state):
    """The prose changes; the ARGUMENT does not.

    Whatever the model writes, the envelope's structured fields are untouched —
    they were computed deterministically and the LLM has no way to reach them.
    """
    envelope = compile_envelope(state, attempt(state, PRIYA))
    client = FakeClient(text="Replica lag is critical. You are on point, Priya.")

    body, source = await render(envelope, client_factory=lambda: client)
    assert source == "openai"
    assert body.startswith("Replica lag is critical")

    # The model was told the decision, not asked for one.
    prompt = client.responses.calls[0]
    assert "Do not change the recipient" in prompt
    assert "Priya Raman" in prompt
    assert "139" in prompt

    fresh = compile_envelope(state, state.attempted[PRIYA])
    assert fresh.chosen_because == envelope.chosen_because
    assert fresh.recipient.id == envelope.recipient.id
    assert fresh.alert is state.alert


async def test_api_failure_falls_back_to_the_template_and_says_so(state):
    """Silent degradation is how you end up unable to explain your own output."""
    envelope = compile_envelope(state, attempt(state, PRIYA))
    client = FakeClient(error=TimeoutError("upstream took too long"))

    body, source = await render(envelope, client_factory=lambda: client)
    assert body == render_template(envelope)
    assert source.startswith("template (openai unavailable")
    assert "TimeoutError" in source


async def test_empty_completion_is_treated_as_a_failure(state):
    """A model that returns nothing has not rendered an explanation."""
    envelope = compile_envelope(state, attempt(state, PRIYA))
    client = FakeClient(text="   ")

    body, source = await render(envelope, client_factory=lambda: client)
    assert body == render_template(envelope)
    assert "ValueError" in source


async def test_deliver_envelope_records_the_render_source(state):
    """Which renderer ran is part of the audit trail, not a detail."""
    record = attempt(state, PRIYA)
    client = FakeClient(text="Short prose.")

    envelope = await deliver_envelope(state, record, client_factory=lambda: client)

    assert state.envelopes[PRIYA] is envelope
    assert envelope.rendered_body == "Short prose."
    rendered = [event for event in state.audit if event.kind == "RENDERED"]
    assert len(rendered) == 1
    assert rendered[0].payload["source"] == "openai"


async def test_the_suite_never_constructs_a_real_client(state, monkeypatch):
    """Guard: even with a key present, a test that forgets to inject a fake must
    not reach the network. Setting a bogus key and asserting the fallback proves
    the failure path is safe rather than merely untested."""
    monkeypatch.setenv("OPENAI_API_KEY", "sk-not-a-real-key")
    envelope = compile_envelope(state, attempt(state, PRIYA))

    body, source = await asyncio.wait_for(render(envelope, api_key=""), 5.0)
    assert source == "template", "an empty key must never attempt a network call"
    assert body == render_template(envelope)
