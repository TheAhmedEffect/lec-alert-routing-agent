"""
Context synthesis and the explanation layer — invariant I1, made visible.

WHAT THE BRIEF ASKS FOR, LITERALLY
----------------------------------
    "...the final recipient has full alert context and an explanation of why
     they were chosen over others."

Two obligations, and this module owns both:

  CONTEXT      the ORIGINAL AlertEvent — the same object, carried by reference
               through every re-route — plus the complete audit trail. Not a
               summary, not a reconstruction. compile_envelope() never builds a
               new AlertEvent, and the test asserts object IDENTITY rather than
               equality, because equality would pass for a faithful copy and a
               faithful copy is exactly the bug I1 exists to catch.

  EXPLANATION  the arithmetic. "Chosen over Tom Beckett because primary domain
               100 + L3 seniority 24 + on-call 15 = 139 against his 123." Every
               number in that sentence was computed by ranking.py and every one
               of them is checkable.

WHAT THE LLM DOES, AND WHAT IT MUST NEVER DO
--------------------------------------------
It writes prose about a decision that has ALREADY been made. It is handed a
finished envelope — recipient, channel and role all fixed — and asked to phrase
it. It gets no tools, no candidate list, and no authority to change anything.

That is not timidity, it is testability: if the routing decision could vary with
model output, the system would be non-deterministic, the eleven-row truth table
would stop meaning anything, and "explain and defend every decision" — the thing
the brief says it is grading — would become impossible.

There is a deterministic template renderer behind it, so the entire system runs
identically with no API key. The tests assert on the template and never touch
the network.
"""

from __future__ import annotations

import os
from typing import Callable

from . import config
from .schemas import (
    AttemptRecord,
    NotificationEnvelope,
    RankedCandidate,
)
from .state import DispatchState

#: Injected by tests. Returns something with `.responses.create(...)`.
ClientFactory = Callable[[], object]


# ─────────────────────────────────────────────────────────────────────────────
# Context compilation — the structured half
# ─────────────────────────────────────────────────────────────────────────────


def _runner_up(
    state: DispatchState, chosen: RankedCandidate
) -> RankedCandidate | None:
    """The strongest ladder member who is NOT the chosen one.

    Ranked the same way the ladder is — reachable first, then qualification —
    because "who would we have picked instead" is the same question the ladder
    already answered.
    """
    others = [
        candidate
        for candidate in state.plan.ladder
        if candidate.snapshot.stakeholder.id != chosen.snapshot.stakeholder.id
    ]
    if not others:
        return None
    return min(others, key=lambda c: c.rank)


def _chosen_because(
    state: DispatchState, chosen: RankedCandidate, attempt: AttemptRecord
) -> str:
    """The one sentence a recipient actually needs, with real numbers in it.

    `score.notes` already holds the terms as ranking.py computed them
    ("primary domain +100", "L3 +24", "on-call +15"), so this is a rendering of
    the arithmetic rather than a re-derivation of it. Nothing here can disagree
    with the ladder, because nothing here recalculates.
    """
    terms = " + ".join(chosen.score.notes) or "no scoring terms"
    line = f"{terms} = {chosen.score.qualification:g}"

    if attempt.role == "escalation":
        # An escalation is justified against the person we are escalating ABOVE,
        # not against the runner-up — the question is "why ALSO you", not
        # "why you".
        #
        # NOTE `attempted`, NOT JUST `notified`. Escalations run in parallel with
        # the incumbent's dispatch, and in row R4 the escalation frequently
        # commits FIRST — the incumbent is still mid-channel-failover and has not
        # reached `notified` yet. Reading only `notified` silently dropped the
        # one comparison this recipient most needs, and did so intermittently,
        # depending on which task won the race.
        peers = [
            candidate
            for candidate in state.plan.ladder
            if candidate.snapshot.stakeholder.id != chosen.snapshot.stakeholder.id
            and (
                candidate.snapshot.stakeholder.id in state.notified
                or candidate.snapshot.stakeholder.id in state.attempted
            )
        ]
        if peers:
            other = max(peers, key=lambda c: c.score.qualification)
            return (
                f"{line}; paged in parallel because that out-qualifies "
                f"{other.snapshot.stakeholder.name} at "
                f"{other.score.qualification:g} on a "
                f"{state.alert.severity.value} alert"
            )
        return f"{line}; paged as a parallel escalation"

    runner_up = _runner_up(state, chosen)
    if runner_up is None:
        return f"{line}; the only eligible candidate"
    return (
        f"{line}, against {runner_up.snapshot.stakeholder.name} at "
        f"{runner_up.score.qualification:g} "
        f"({runner_up.snapshot.stakeholder.domain_fit(state.alert.domain)} domain)"
    )


def _considered_and_passed(
    state: DispatchState, chosen: RankedCandidate
) -> tuple[tuple[str, str], ...]:
    """Everyone deliberately passed over, WITH the arithmetic that passed them.

    `state.suppressed` already holds strings like

        "Tom Beckett qualification 108 < floor 120 (max of CRITICAL minimum 120,
         incumbent Priya Raman 139 - tolerance 25)"

    They are handed through unchanged. Summarising them here would throw away
    the only part a reviewer can check.
    """
    passed: list[tuple[str, str]] = []
    for person_id, reason in state.suppressed.items():
        candidate = state.candidate_for(person_id)
        name = candidate.snapshot.stakeholder.name if candidate else person_id
        passed.append((name, reason))

    # Anyone we tried and could not reach also belongs in the record: from the
    # recipient's point of view "we tried Priya first and it failed" is context,
    # not noise.
    for person_id, record in state.attempted.items():
        if person_id == chosen.snapshot.stakeholder.id or person_id in state.notified:
            continue
        if person_id in state.suppressed:
            continue
        # Somebody still mid-dispatch has NOT been passed over — they are being
        # contacted right now. Row R4 holds the incumbent on a persistent channel
        # while escalating in parallel, so at the moment the escalation's
        # envelope is compiled the incumbent is legitimately still `connecting`.
        # Listing them as "considered and passed" would be a plain falsehood.
        if record.state.is_pre_commit:
            continue
        candidate = state.candidate_for(person_id)
        name = candidate.snapshot.stakeholder.name if candidate else person_id
        passed.append(
            (name, f"attempt {record.state.value}: {record.outcome_reason or 'no reason recorded'}")
        )
    return tuple(passed)


def compile_envelope(
    state: DispatchState, attempt: AttemptRecord
) -> NotificationEnvelope:
    """Everything one recipient gets, for ONE attempt.

    Per-attempt rather than per-alert on purpose: row R4 notifies two people
    with different roles on different channels — Priya as `primary` after a
    failover, Elena as `escalation` — and they need genuinely different
    envelopes, not two copies of one.
    """
    candidate = state.candidate_for(attempt.stakeholder_id)
    if candidate is None:
        raise ValueError(
            f"{attempt.stakeholder_id} is not in the ladder — an envelope for a "
            "non-member would describe a routing decision that never happened"
        )

    return NotificationEnvelope(
        # BY REFERENCE. Not a copy, not a rebuild. This is invariant I1.
        alert=state.alert,
        recipient=candidate.snapshot.stakeholder,
        channel=attempt.channel,
        role=attempt.role,
        plan_version=attempt.plan_version,
        chosen_because=_chosen_because(state, candidate, attempt),
        considered_and_passed=_considered_and_passed(state, candidate),
        audit_trail=state.audit_lines(),
        rendered_body="",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Rendering — the prose half
# ─────────────────────────────────────────────────────────────────────────────


def render_template(envelope: NotificationEnvelope) -> str:
    """The deterministic renderer. Always available, no key, no network.

    This is the DEFAULT, not the fallback-of-last-resort, and it is what the
    tests assert on. Everything the recipient needs is here; the LLM only ever
    rephrases it.
    """
    alert = envelope.alert
    lines = [
        f"[{alert.severity.value.upper()}] {alert.describe()}",
        f"breach magnitude: {alert.breach_magnitude:.0%} past threshold",
        "",
        f"You are the {envelope.role} recipient, contacted on "
        f"{envelope.channel.value}.",
        "",
        f"Why you: {envelope.chosen_because}",
    ]

    if envelope.considered_and_passed:
        lines += ["", "Considered and passed over:"]
        lines += [f"  - {name}: {why}" for name, why in envelope.considered_and_passed]

    if envelope.audit_trail:
        lines += ["", "What happened:"]
        lines += [f"  {line}" for line in envelope.audit_trail]

    return "\n".join(lines)


def _prompt(envelope: NotificationEnvelope) -> str:
    """The model is TOLD the decision. It is never asked to make one.

    Note what is absent: no candidate list to pick from, no scores to weigh, no
    question about who should be notified. The recipient, channel and role are
    stated as facts.
    """
    return (
        "You are writing a short operational notification. The routing decision "
        "has already been made by a deterministic system; your ONLY job is to "
        "phrase it clearly for the person receiving it.\n\n"
        "Do not change the recipient, the channel or the role. Do not suggest "
        "an alternative recipient. Do not invent facts. Use only what is below.\n\n"
        f"ALERT: {envelope.alert.describe()}\n"
        f"RECIPIENT: {envelope.recipient.name}, {envelope.recipient.title}\n"
        f"CHANNEL: {envelope.channel.value}\n"
        f"ROLE: {envelope.role}\n"
        f"WHY THIS PERSON: {envelope.chosen_because}\n"
        "PASSED OVER: "
        + (
            "; ".join(f"{n} ({w})" for n, w in envelope.considered_and_passed)
            or "nobody"
        )
        + "\n"
        f"WHAT HAPPENED:\n" + "\n".join(envelope.audit_trail) + "\n\n"
        "Write at most 120 words, plain text, no markdown headings. Lead with "
        "what broke and what is being asked of them. Include the numeric "
        "justification verbatim."
    )


async def render(
    envelope: NotificationEnvelope,
    *,
    client_factory: ClientFactory | None = None,
    api_key: str | None = None,
) -> tuple[str, str]:
    """Return (body, source). `source` is 'template' or 'openai'.

    THE FALLBACK ANNOUNCES ITSELF. Every failure path returns a source string
    naming what went wrong, and the caller writes it to the audit trail. Silent
    degradation is how you end up on camera unable to explain your own output.
    """
    key = api_key if api_key is not None else os.getenv("OPENAI_API_KEY", "")
    if not key and client_factory is None:
        return render_template(envelope), "template"

    try:
        if client_factory is not None:
            client = client_factory()
        else:
            # Imported lazily so the package works with openai uninstalled.
            from openai import AsyncOpenAI  # noqa: PLC0415

            client = AsyncOpenAI(timeout=config.OPENAI_TIMEOUT_SECONDS)

        # openai 3.x Responses API: `input=` in, `.output_text` out. Deliberately
        # minimal parameters — temperature and token caps vary by model and an
        # unsupported keyword would raise TypeError, which the except below would
        # silently turn into a template render. Fewer knobs, fewer silent
        # downgrades.
        response = await client.responses.create(
            model=config.OPENAI_MODEL, input=_prompt(envelope)
        )
        body = (getattr(response, "output_text", "") or "").strip()
        if not body:
            raise ValueError("model returned an empty completion")
        return body[: config.MAX_EXPLANATION_CHARS], "openai"

    except Exception as exc:  # noqa: BLE001 - any failure degrades to the template
        return (
            render_template(envelope),
            f"template (openai unavailable: {type(exc).__name__})",
        )


async def deliver_envelope(
    state: DispatchState,
    attempt: AttemptRecord,
    *,
    client_factory: ClientFactory | None = None,
) -> NotificationEnvelope:
    """Compile, render, record. Called by the executor once a send has committed.

    Deliberately AFTER the commit: rendering is presentation, and nothing about
    presentation should be able to delay or fail a notification that has already
    been decided.
    """
    envelope = compile_envelope(state, attempt)
    body, source = await render(envelope, client_factory=client_factory)
    envelope = envelope.model_copy(update={"rendered_body": body})
    state.envelopes[attempt.stakeholder_id] = envelope

    await state.record_audit(
        "RENDERED",
        attempt.stakeholder_id,
        f"explanation rendered via {source} for "
        f"{envelope.recipient.name} ({envelope.role})",
        {"source": source, "chars": len(body)},
    )
    return envelope
