"""
Gate 5 verification — the explanation the recipient actually receives.

Runs Appendix A's `floor` scenario and prints both envelopes in full: Priya's
(primary, after a channel failover) and Elena's (escalation). Section 3 is what
goes on screen at 2:15 in the walkthrough.

    python verify_module5.py

Runs with or without OPENAI_API_KEY. With no key it uses the deterministic
template and says so; the structured argument is identical either way.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

from alert_router.agent import AlertAgent
from alert_router.channels import ChannelBank
from alert_router.context import compile_envelope, render_template
from alert_router.db import build_engine, build_session_factory, init_db
from alert_router.executor import PhaseHooks
from alert_router.registry import PresenceBus, Registry, zero_latency
from alert_router.schemas import AlertEvent, Availability, InterruptKind, Severity

DB_FILE = Path(__file__).resolve().parent / "verify_module5.db"
RESULTS: list[bool] = []
PRIYA, TOM, ELENA = "stk-001", "stk-002", "stk-003"


def check(label: str, ok: bool, detail: str = "") -> None:
    RESULTS.append(ok)
    print(f"  [{'PASS' if ok else 'FAIL'}] {label:<54} {detail}")


def section(title: str) -> None:
    print(f"\n{title}\n{'-' * len(title)}")


class Gate:
    def __init__(self) -> None:
        self.reached = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, attempt) -> None:
        self.reached.set()
        await self.release.wait()


async def main() -> int:
    for suffix in ("", "-wal", "-shm"):
        Path(str(DB_FILE) + suffix).unlink(missing_ok=True)

    engine = build_engine(f"sqlite+aiosqlite:///{DB_FILE.as_posix()}")
    sessions = build_session_factory(engine)
    await init_db(engine)

    section("0. Renderer")
    has_key = bool(os.getenv("OPENAI_API_KEY"))
    print(f"  OPENAI_API_KEY present: {has_key}")
    print("  (the structured argument below is identical either way — the LLM")
    print("   only rephrases a decision that has already been made)")

    bus = PresenceBus()
    registry = Registry(sessions, bus=bus, latency=zero_latency)
    bank = ChannelBank(connect_seconds=0.0, send_seconds=0.0)
    gate = Gate()
    seen = asyncio.Event()

    await registry.set_on_call(TOM, False)      # Tom to 108: the `floor` scenario

    alert = AlertEvent(
        alert_id="alr-verify-5",
        metric_name="db_replica_lag_seconds",
        value=94.0,
        threshold=30.0,
        severity=Severity.CRITICAL,
        domain="infrastructure",
    )

    agent = AlertAgent(registry, sessions, bank, hooks=PhaseHooks(on_in_flight=gate))
    inner = agent.on_interrupt

    async def watched(state, event, attempt):
        decision = await inner(state, event, attempt)
        if event.kind is InterruptKind.PRESENCE_CHANGED:
            seen.set()
        return decision

    agent.on_interrupt = watched

    section("1. Running scenario `floor`")
    task = asyncio.create_task(agent.handle(alert))
    await asyncio.wait_for(gate.reached.wait(), 5.0)
    await registry.set_status(PRIYA, Availability.OFFLINE, reason="laptop closed")
    await asyncio.wait_for(seen.wait(), 5.0)
    gate.release.set()
    state = await asyncio.wait_for(task, 15.0)
    print(f"  notified: {sorted(state.notified)}")
    print(f"  matrix rows fired: {[d.matrix_row for d in agent.decisions]}")

    section("2. One envelope per recipient")
    check("two envelopes compiled", len(state.envelopes) == 2,
          str(sorted(state.envelopes)))
    roles = {sid: env.role for sid, env in state.envelopes.items()}
    check("different roles", len(set(roles.values())) == 2, str(roles))
    check("both carry the SAME alert object (invariant I1)",
          all(env.alert is state.alert for env in state.envelopes.values()))

    for stakeholder_id in sorted(state.envelopes):
        envelope = state.envelopes[stakeholder_id]
        section(f"3. Envelope for {envelope.recipient.name} ({envelope.role})")
        print(f"  channel      : {envelope.channel.value}")
        print(f"  why you      : {envelope.chosen_because}")
        if envelope.considered_and_passed:
            print("  passed over  :")
            for name, why in envelope.considered_and_passed:
                print(f"     - {name}: {why[:92]}")
        print(f"  rendered body ({len(envelope.rendered_body)} chars):")
        for line in envelope.rendered_body.splitlines()[:14]:
            print(f"     {line[:100]}")

    section("4. The explanation is checkable")
    priya_envelope = state.envelopes.get(PRIYA)
    elena_envelope = state.envelopes.get(ELENA)
    check("Priya's envelope shows her arithmetic",
          priya_envelope is not None and "139" in priya_envelope.chosen_because)
    check("Elena's escalation justifies itself against the incumbent",
          elena_envelope is not None and "140" in elena_envelope.chosen_because
          and "Priya" in elena_envelope.chosen_because)
    if priya_envelope is not None:
        passed = dict(priya_envelope.considered_and_passed)
        check("Tom appears with the numeric refusal",
              "Tom Beckett" in passed
              and all(t in passed["Tom Beckett"] for t in ("108", "120", "139")))
        check("the audit trail travels with the envelope",
              len(priya_envelope.audit_trail) >= 8,
              f"{len(priya_envelope.audit_trail)} lines")

    section("5. The template renderer stands alone")
    if priya_envelope is not None:
        template = render_template(priya_envelope)
        check("template contains the full argument with no LLM",
              all(token in template for token in ("139", "Why you", "What happened")))
        check("template names the passed-over candidates",
              "Tom Beckett" in template)
    rendered = [e for e in state.audit if e.kind == "RENDERED"]
    check("every render records which path produced it", len(rendered) == 2,
          ", ".join(e.payload.get("source", "?") for e in rendered))

    section("6. Recompiling is deterministic")
    if priya_envelope is not None:
        again = compile_envelope(state, state.attempted[PRIYA])
        check("the structured argument does not drift",
              again.chosen_because == priya_envelope.chosen_because)
        check("and still carries the original alert", again.alert is state.alert)

    bus.close()
    await engine.dispose()
    for suffix in ("", "-wal", "-shm"):
        Path(str(DB_FILE) + suffix).unlink(missing_ok=True)

    print(f"\n{'=' * 74}")
    print(f"  {sum(RESULTS)}/{len(RESULTS)} checks passed")
    print(f"{'=' * 74}")
    return 0 if all(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
