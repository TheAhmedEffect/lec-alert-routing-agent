"""
The command-line demo — what the reviewer runs and what the camera records.

    python -m alert_router.cli demo --scenario all
    python -m alert_router.cli demo --scenario floor
    python -m alert_router.cli matrix
    python -m alert_router.cli invariants

DESIGN CONSTRAINTS THAT ARE NOT COSMETIC
----------------------------------------
The console is pinned to 100 columns. Rich panels look excellent at native
resolution and turn to mush at 1080p, which is where this will actually be
watched — and the most important thing on screen is a 108-character line:

    Tom Beckett qualification 108 < floor 120 (max of CRITICAL minimum 120, ...)

Nothing is distinguished by COLOUR ALONE. Every suppression, every decision and
every delivery carries a text label, because a reviewer watching a compressed
recording on a laptop should not have to tell grey from slightly different grey.

Rich markup is DISABLED on this console. Everything printed here is data, and a
markup parser turned loose on data deletes whatever happens to look like a tag —
see the comment on `console` below.
"""

from __future__ import annotations

import asyncio
import textwrap
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table
from rich.text import Text

from .decisions import MATRIX
from .scenarios import SCENARIO_NAMES, ScenarioResult, cleanup, run_all, run_scenario

app = typer.Typer(add_completion=False, help="Alert routing agent — demo and inspection.")
# markup=False is not cosmetic. Rich parses `[...]` as style markup, and its tag
# regex matches anything starting with a lowercase letter, `#`, `/` or `@`. The
# alert descriptor ends in `[critical/infrastructure]` — lowercase — so Rich read
# it as a style tag and silently DELETED it from the recipient's message, while
# `[00]`, `[R3]` and `[CRITICAL]` survived because digits and capitals do not
# match. Every string printed here is data, not markup, so the parser has nothing
# legitimate to do and everything to lose. Disabling it console-wide also covers
# the tables, where stakeholder names and suppression reasons are equally at the
# mercy of whatever punctuation the data happens to contain.
console = Console(width=100, highlight=False, markup=False)

RULE = "─" * 96


class _Sections:
    """Numbers the sections as they are PRINTED, not as they are written.

    Scenarios with no suppressions skip section 5, and a hardcoded number then
    leaves a visible gap — "4. DECISIONS" followed by "6. RESULT". Small, but it
    is the kind of thing a reviewer notices on a recording and quietly reads as
    carelessness.
    """

    def __init__(self) -> None:
        self.n = 0

    def __call__(self, title: str) -> None:
        self.n += 1
        console.print()
        console.print(Text(f"{self.n}. {title}", style="bold"))
        console.print(RULE, style="dim")


def _heading(text: str) -> None:
    console.print()
    console.print(Text(text, style="bold"))
    console.print(RULE, style="dim")


def _wrapped(text: str, *, indent: str = "       ", width: int = 86) -> None:
    """Wrap rather than truncate.

    The rationales and suppression reasons are the most quotable lines in the
    whole demo — "qualification 108 < floor 120 (max of CRITICAL minimum 120,
    incumbent Priya Raman 139 - tolerance 25)" is the argument. Cutting it at 88
    characters throws away the half that makes it convincing.
    """
    for line in textwrap.wrap(text, width=width) or [""]:
        console.print(f"{indent}{line}", style="dim")


def _render_ladder(result: ScenarioResult) -> None:
    table = Table(show_edge=False, pad_edge=False, box=None, padding=(0, 1))
    table.add_column("rank", width=4, justify="right")
    table.add_column("name", width=16)
    table.add_column("domain fit", width=10)
    table.add_column("tier", width=4)
    table.add_column("on-call", width=7)
    table.add_column("status", width=7)
    table.add_column("reach", width=5, justify="right")
    table.add_column("QUAL", width=5, justify="right")
    table.add_column("outcome", width=22)

    for candidate in result.plan.ladder:
        person = candidate.snapshot.stakeholder
        record = result.state.attempted.get(person.id)
        # An empty cell reads as "the demo forgot about this person". It is
        # actually the most interesting row on the ladder: Elena out-qualifies
        # everybody and the walk simply stopped before reaching her.
        outcome = record.state.value if record else "not attempted"
        if person.id in result.state.notified:
            outcome = f"NOTIFIED ({record.role})" if record else "NOTIFIED"
        elif person.id in result.state.suppressed:
            outcome = "SUPPRESSED"
        table.add_row(
            str(candidate.rank),
            person.name,
            person.domain_fit(result.alert.domain),
            f"L{person.seniority_tier}",
            "yes" if person.on_call else "no",
            candidate.snapshot.status.value,
            str(candidate.score.reachability),
            f"{candidate.score.qualification:.0f}",
            outcome,
        )
    console.print(table)


def _render_scenario(result: ScenarioResult) -> None:
    section = _Sections()
    console.print()
    console.print(Text(f"SCENARIO  {result.name}", style="bold reverse"))
    console.print(Text(f"          {result.headline}", style="dim"))

    section("THE ALERT")
    alert = result.alert
    console.print(f"  {alert.describe()}")
    console.print(
        f"  direction={alert.direction}  "
        f"breach={alert.breach_magnitude:.0%} "
        f"{'below' if alert.direction == 'below' else 'past'} threshold  "
        f"id={alert.alert_id}"
    )
    if alert.direction == "below":
        console.print(
            "  NOTE  this breach crosses the threshold by FALLING — a router that",
            style="dim",
        )
        console.print(
            "        hardcodes 'value > threshold' would drop it silently.", style="dim"
        )

    section(f"THE LADDER  —  one query, {len(result.plan.ladder)} candidates")
    _render_ladder(result)
    console.print(
        "  QUAL contains no availability term. `reach` is the separate axis.",
        style="dim",
    )

    section("WHAT HAPPENED")
    for line in result.state.audit_lines():
        console.print(f"  {line[:94]}")

    section("DECISIONS TAKEN")
    if result.decisions:
        for decision in result.decisions:
            console.print(f"  [{decision.matrix_row}] {decision.action.value}")
            _wrapped(decision.rationale)
    else:
        console.print("  (no interrupt reached the matrix)", style="dim")

    if result.state.suppressed:
        section("SUPPRESSED  —  declined, with the arithmetic")
        for person_id, why in result.state.suppressed.items():
            candidate = result.state.candidate_for(person_id)
            name = candidate.snapshot.stakeholder.name if candidate else person_id
            console.print(f"  SUPPRESSED  {name}")
            _wrapped(why, indent="              ", width=80)

    section("RESULT")
    for person_id in result.notified:
        candidate = result.state.candidate_for(person_id)
        record = result.state.attempted.get(person_id)
        name = candidate.snapshot.stakeholder.name if candidate else person_id
        console.print(
            f"  NOTIFIED    {name:<16} role={record.role:<11} "
            f"channel={record.channel.value}"
        )
    console.print(f"  DELIVERED   {result.delivered}")
    console.print(
        f"  QUERIES     {result.query_count}  "
        f"(one per candidate, and it never moved during dispatch)"
    )
    console.print(
        f"  MATRIX      expected {result.expected_row}, fired {result.rows_fired}  "
        f"{'OK' if result.matched_expectation else 'MISMATCH'}"
    )

    envelope = next(iter(result.state.envelopes.values()), None)
    if envelope is not None:
        section("WHAT THE RECIPIENT RECEIVES")
        console.print(f"  to      {envelope.recipient.name} ({envelope.role})")
        console.print("  why you")
        _wrapped(envelope.chosen_because, indent="          ", width=84)
        console.print("  message")
        # Wrapped, not sliced. This block is the evidence for invariant I1 —
        # cutting it mid-word is the one place a viewer cannot tell "truncated
        # for the demo" from "the context was lost".
        printed = 0
        for line in envelope.rendered_body.splitlines():
            if printed >= 8:
                console.print("          ...", style="dim")
                break
            for wrapped in textwrap.wrap(line, width=84) or [""]:
                console.print(f"          {wrapped}", style="dim")
                printed += 1


# ─────────────────────────────────────────────────────────────────────────────
# Commands
# ─────────────────────────────────────────────────────────────────────────────


@app.command()
def demo(
    scenario: str = typer.Option(
        "all", "--scenario", "-s", help=f"one of {', '.join(SCENARIO_NAMES)}, or 'all'"
    ),
    send_seconds: float = typer.Option(
        0.0, "--send-seconds", help="simulated transport latency; raise it to slow the demo"
    ),
) -> None:
    """Run one scenario, or all four, each on its own clean database."""
    names = list(SCENARIO_NAMES) if scenario == "all" else [scenario]
    for name in names:
        if name not in SCENARIO_NAMES:
            console.print(f"unknown scenario {name!r}; choose from {SCENARIO_NAMES}")
            raise typer.Exit(code=2)

    results: list[ScenarioResult] = []
    try:
        for name in names:
            result = asyncio.run(run_scenario(name, send_seconds=send_seconds))
            _render_scenario(result)
            results.append(result)
    finally:
        cleanup()

    if len(results) > 1:
        _heading("SUMMARY")
        table = Table(show_edge=False, box=None, padding=(0, 2))
        table.add_column("scenario", width=10)
        table.add_column("row", width=6)
        table.add_column("notified", width=30)
        table.add_column("queries", width=7, justify="right")
        table.add_column("result", width=8)
        for result in results:
            names_notified = ", ".join(
                result.state.candidate_for(p).snapshot.stakeholder.name.split()[0]
                for p in result.notified
                if result.state.candidate_for(p)
            )
            table.add_row(
                result.name,
                result.expected_row,
                names_notified or "(none)",
                str(result.query_count),
                "OK" if result.matched_expectation else "MISMATCH",
            )
        console.print(table)

    if not all(r.matched_expectation for r in results):
        raise typer.Exit(code=1)


@app.command()
def matrix() -> None:
    """Print the decision matrix. Order is the specification: first match wins."""
    _heading("DECISION MATRIX  —  evaluated top to bottom, first match wins")
    for row_id, predicate, _ in MATRIX:
        doc = (predicate.__doc__ or "").strip().splitlines()
        summary = doc[0] if doc else predicate.__name__
        console.print(f"  {row_id:<4} {summary[:88]}")
    console.print()
    console.print(
        "  An event matching no row RAISES. A silent default would make every",
        style="dim",
    )
    console.print("  future gap in this table invisible.", style="dim")


@app.command()
def invariants() -> None:
    """Run every scenario and check all four invariants against each."""
    results = asyncio.run(run_all())
    try:
        _heading("INVARIANTS  —  checked against every scenario")
        table = Table(show_edge=False, box=None, padding=(0, 2))
        table.add_column("scenario", width=10)
        table.add_column("I1 context", width=11)
        table.add_column("I2 no dupes", width=12)
        table.add_column("I3 one query", width=13)
        table.add_column("I4 no downgrade", width=16)
        ok = True
        for result in results:
            state = result.state
            i1 = all(e.alert is state.alert for e in state.envelopes.values())
            i2 = len(state.notified) == len(set(state.notified))
            i3 = result.query_count == len(state.evaluated)
            i4 = _no_downward_escalation(result)
            ok = ok and i1 and i2 and i3 and i4
            table.add_row(
                result.name,
                "PASS" if i1 else "FAIL",
                "PASS" if i2 else "FAIL",
                "PASS" if i3 else "FAIL",
                "PASS" if i4 else "FAIL",
            )
        console.print(table)
        console.print()
        console.print(f"  {'ALL INVARIANTS HOLD' if ok else 'INVARIANT VIOLATED'}")
    finally:
        cleanup()
    if not ok:
        raise typer.Exit(code=1)


def _no_downward_escalation(result: ScenarioResult) -> bool:
    """Nobody notified may be less qualified than somebody suppressed for being
    under-qualified. That is invariant I4, asserted against the outcome rather
    than against the scoring function."""
    state = result.state
    notified_scores = [
        c.score.qualification
        for c in state.plan.ladder
        if c.snapshot.stakeholder.id in state.notified
    ]
    suppressed_scores = [
        c.score.qualification
        for c in state.plan.ladder
        if c.snapshot.stakeholder.id in state.suppressed
    ]
    if not notified_scores or not suppressed_scores:
        return True
    return min(notified_scores) > max(suppressed_scores)


def main() -> None:  # pragma: no cover - entry point
    app()


if __name__ == "__main__":  # pragma: no cover
    main()
