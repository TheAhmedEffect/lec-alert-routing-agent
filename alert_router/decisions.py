"""
The decision matrix — eleven ordered rows, first match wins.

WHY THIS IS A SEQUENCE AND NOT NESTED `if` BLOCKS
-------------------------------------------------
The ORDER is the specification. R2 and R3 fire on the identical event and differ
only in whether a replacement exists; R2 and R5 differ only in phase. Written as
nested conditionals, one row shadows another silently — and the failure is
invisible, because the wrong row still returns a plausible action and the system
keeps working in a way nobody notices until it matters.

Written as a literal ordered tuple, the table in the plan and the code below can
be read side by side, and `matrix_row` on every decision makes the audit trail
traceable back to a specific line.

WHY THIS MODULE IS PURE
-----------------------
`decide()` takes facts and returns a decision. No session, no executor, no I/O
await. Every effect lives in `agent.apply()`.

That split is what makes the whole truth table testable in milliseconds without a
database, an event loop or a channel adapter — which in turn is what makes it
practical to test ALL ELEVEN rows rather than the three that are easy.

The one fact the matrix needs from the database — which transports are currently
healthy for the incumbent — is resolved by the caller and passed in as
`ChannelFacts`. That keeps the impurity at the boundary where it belongs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from .ranking import best_by_qualification, clears_floor
from .schemas import (
    AttemptRecord,
    Channel,
    DecisionAction,
    InterruptEvent,
    InterruptKind,
    RankedCandidate,
    RoutingDecision,
    Severity,
)
from .state import DispatchState


class NoMatchingRow(RuntimeError):
    """No row in the matrix matched.

    This is deliberately a crash rather than a default. Falling through to
    CONTINUE_UNCHANGED would make every future gap in the table invisible: the
    system would quietly do nothing, forever, and no test would ever notice.
    A missing row is a bug in the matrix, and bugs should be loud.
    """


@dataclass(frozen=True)
class ChannelFacts:
    """Transport health for the incumbent, resolved by the caller.

    decisions.py must not touch the database, so the agent looks these up from
    `channel_health` before calling decide(). Defaulting to empty means a caller
    that forgets will see rows R6/R7 behave as "no healthy transport", which
    fails safe — it re-routes rather than silently continuing down a dead pipe.
    """

    healthy: tuple[Channel, ...] = ()
    persistent: Channel | None = None


@dataclass(frozen=True)
class Ctx:
    """Everything the predicates need, computed once.

    Building this up front keeps each predicate to a single readable line, which
    is the point: a reviewer should be able to check the code against §3.5
    without holding anything in their head.
    """

    state: DispatchState
    event: InterruptEvent
    attempt: AttemptRecord | None
    channels: ChannelFacts

    subject: RankedCandidate | None = None
    incumbent: RankedCandidate | None = None
    about_incumbent: bool = False
    pre_commit: bool = False
    in_flight_channel: Channel | None = None
    replacement: RankedCandidate | None = None
    replacement_reason: str = ""
    escalation_target: RankedCandidate | None = None
    rejected: tuple[tuple[str, str], ...] = field(default_factory=tuple)

    @property
    def severity(self) -> Severity:
        return self.state.alert.severity


def _build_ctx(
    state: DispatchState,
    event: InterruptEvent,
    attempt: AttemptRecord | None,
    channels: ChannelFacts,
) -> Ctx:
    subject = state.candidate_for(event.stakeholder_id)
    incumbent = (
        state.candidate_for(attempt.stakeholder_id) if attempt is not None else None
    )
    about_incumbent = (
        attempt is not None and attempt.stakeholder_id == event.stakeholder_id
    )
    pre_commit = attempt is not None and attempt.state.is_pre_commit

    # Who could take over? Only REACHABLE ladder members who have not been tried
    # and who clear the floor. Reachability matters here because a replacement
    # has to actually answer; qualification decides whether they are ALLOWED to.
    replacement: RankedCandidate | None = None
    replacement_reason = ""
    rejected: list[tuple[str, str]] = []
    for candidate in state.remaining:
        if candidate.snapshot.stakeholder.id == event.stakeholder_id:
            continue
        observed = state.observed.get(candidate.snapshot.stakeholder.id)
        if observed is None or observed.status.reachability == 0:
            continue
        ok, reason = clears_floor(candidate, incumbent, state.alert.severity)
        if ok and replacement is None:
            replacement, replacement_reason = candidate, reason
        elif not ok:
            # The numeric refusal. THIS is what appears on screen in the video,
            # and it is the difference between "skipped" and "declined, here is
            # the arithmetic".
            rejected.append((candidate.snapshot.stakeholder.id, reason))

    # Escalating UP ignores reachability: a persistent channel reaches someone
    # who is away from their desk, which is exactly what row R4 relies on.
    untried = [
        candidate
        for candidate in state.remaining
        if candidate.snapshot.stakeholder.id != event.stakeholder_id
    ]
    escalation_target = best_by_qualification(untried)

    return Ctx(
        state=state,
        event=event,
        attempt=attempt,
        channels=channels,
        subject=subject,
        incumbent=incumbent,
        about_incumbent=about_incumbent,
        pre_commit=pre_commit,
        in_flight_channel=attempt.channel if attempt is not None else None,
        replacement=replacement,
        replacement_reason=replacement_reason,
        escalation_target=escalation_target,
        rejected=tuple(rejected),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Predicates — one line each, readable against §3.5
# ─────────────────────────────────────────────────────────────────────────────


def _is_presence_drop(ctx: Ctx) -> bool:
    return (
        ctx.event.kind is InterruptKind.PRESENCE_CHANGED and ctx.event.went_offline
    )


def _r1(ctx: Ctx) -> bool:
    """Not about the person we are currently contacting."""
    if ctx.event.kind is InterruptKind.BETTER_MATCH:
        return False  # derived, and always about somebody else by construction
    return ctx.attempt is None or not ctx.about_incumbent


def _r2(ctx: Ctx) -> bool:
    return (
        _is_presence_drop(ctx)
        and ctx.about_incumbent
        and ctx.pre_commit
        and ctx.in_flight_channel is not None
        and ctx.in_flight_channel.is_persistent
    )


def _r3(ctx: Ctx) -> bool:
    return (
        _is_presence_drop(ctx)
        and ctx.about_incumbent
        and ctx.pre_commit
        and ctx.in_flight_channel is not None
        and not ctx.in_flight_channel.is_persistent
        and ctx.replacement is not None
    )


def _r4(ctx: Ctx) -> bool:
    return (
        _is_presence_drop(ctx)
        and ctx.about_incumbent
        and ctx.pre_commit
        and ctx.in_flight_channel is not None
        and not ctx.in_flight_channel.is_persistent
        and ctx.replacement is None
    )


def _r5(ctx: Ctx) -> bool:
    # Same event as R2 and R3. ONLY the phase differs, and the phase comes from
    # AttemptState.is_pre_commit — never inferred from anything else.
    return _is_presence_drop(ctx) and ctx.about_incumbent and not ctx.pre_commit


def _r6(ctx: Ctx) -> bool:
    return (
        ctx.event.kind is InterruptKind.CHANNEL_DEGRADED
        and ctx.about_incumbent
        and ctx.pre_commit
        and ctx.event.channel == ctx.in_flight_channel
        and bool(_fallback_channel(ctx))
    )


def _r7(ctx: Ctx) -> bool:
    return (
        ctx.event.kind is InterruptKind.CHANNEL_DEGRADED
        and ctx.about_incumbent
        and ctx.pre_commit
        and _fallback_channel(ctx) is None
    )


def _r8(ctx: Ctx) -> bool:
    return (
        ctx.event.kind is InterruptKind.BETTER_MATCH
        and ctx.event.stakeholder_id in ctx.state.notified
    )


def _r9(ctx: Ctx) -> bool:
    return (
        ctx.event.kind is InterruptKind.BETTER_MATCH
        and ctx.subject is not None
        and ctx.incumbent is not None
        and ctx.subject.score.qualification > ctx.incumbent.score.qualification
        and ctx.severity.rank >= Severity.HIGH.rank
    )


def _r10(ctx: Ctx) -> bool:
    return (
        ctx.event.kind is InterruptKind.BETTER_MATCH
        and ctx.severity is Severity.LOW
    )


def _r11(ctx: Ctx) -> bool:
    return not ctx.state.notified and not ctx.state.remaining


def _fallback_channel(ctx: Ctx) -> Channel | None:
    """The best healthy transport that is not the one that just failed."""
    for channel in ctx.channels.healthy:
        if channel != ctx.event.channel and channel != ctx.in_flight_channel:
            return channel
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Handlers
# ─────────────────────────────────────────────────────────────────────────────


def _continue(row: str, why: str):
    def handler(ctx: Ctx) -> RoutingDecision:
        return RoutingDecision(
            action=DecisionAction.CONTINUE_UNCHANGED, rationale=why, matrix_row=row
        )

    return handler


def _h_r2(ctx: Ctx) -> RoutingDecision:
    return RoutingDecision(
        action=DecisionAction.CONTINUE_UNCHANGED,
        matrix_row="R2",
        rationale=(
            f"{ctx.incumbent.snapshot.stakeholder.name} went offline, but "
            f"{ctx.in_flight_channel.value} is persistent — the message waits on "
            f"the device. Offline means away from keyboard, not unreachable."
        ),
    )


def _h_r3(ctx: Ctx) -> RoutingDecision:
    return RoutingDecision(
        action=DecisionAction.ABORT_AND_REROUTE,
        target_id=ctx.replacement.snapshot.stakeholder.id,
        matrix_row="R3",
        rationale=(
            f"{ctx.incumbent.snapshot.stakeholder.name} went offline on a "
            f"synchronous channel pre-commit; {ctx.replacement_reason}"
        ),
        suppressed=ctx.rejected,
    )


def _h_r4(ctx: Ctx) -> RoutingDecision:
    """Invariant I4. THREE obligations, all of them mandatory.

    Keep the incumbent on a persistent channel, page the most qualified member,
    and record every rejected candidate with the arithmetic that rejected them.
    Skipping the third is the common failure, and the third is the one the
    reviewer sees.
    """
    escalate = ctx.escalation_target
    return RoutingDecision(
        action=DecisionAction.HOLD_AND_ESCALATE_UP,
        target_id=ctx.incumbent.snapshot.stakeholder.id,
        target_channel=ctx.channels.persistent,
        escalate_to_id=escalate.snapshot.stakeholder.id if escalate else None,
        matrix_row="R4",
        rationale=(
            f"{ctx.incumbent.snapshot.stakeholder.name} went offline, and no "
            f"reachable candidate clears the floor — refusing to route down. "
            f"Holding on "
            f"{ctx.channels.persistent.value if ctx.channels.persistent else 'no persistent channel'}"
            + (
                f" and escalating up to {escalate.snapshot.stakeholder.name} "
                f"({escalate.score.qualification:g})"
                if escalate
                else ""
            )
        ),
        suppressed=ctx.rejected,
    )


def _h_r5(ctx: Ctx) -> RoutingDecision:
    escalate = ctx.escalation_target
    return RoutingDecision(
        action=DecisionAction.COMPLETE_AND_ESCALATE_PARALLEL,
        target_id=ctx.incumbent.snapshot.stakeholder.id if ctx.incumbent else None,
        escalate_to_id=escalate.snapshot.stakeholder.id if escalate else None,
        matrix_row="R5",
        rationale=(
            "the message is already committed and cannot be unsent; supplementing "
            "with a parallel escalation rather than retracting"
        ),
    )


def _h_r6(ctx: Ctx) -> RoutingDecision:
    fallback = _fallback_channel(ctx)
    return RoutingDecision(
        action=DecisionAction.CHANNEL_FAILOVER,
        target_id=ctx.incumbent.snapshot.stakeholder.id,
        target_channel=fallback,
        matrix_row="R6",
        rationale=(
            f"{ctx.event.channel.value if ctx.event.channel else 'channel'} degraded; "
            f"failing over to {fallback.value if fallback else '?'} for the same "
            f"person. Same idempotency key — one notification down a different pipe, "
            f"not a second notification."
        ),
    )


def _h_r7(ctx: Ctx) -> RoutingDecision:
    return RoutingDecision(
        action=DecisionAction.ABORT_AND_REROUTE,
        target_id=(
            ctx.replacement.snapshot.stakeholder.id if ctx.replacement else None
        ),
        matrix_row="R7",
        rationale=(
            f"no healthy transport remains for "
            f"{ctx.incumbent.snapshot.stakeholder.name}; they are genuinely "
            f"unreachable, so changing person is now correct"
        ),
        suppressed=ctx.rejected,
    )


def _h_r9(ctx: Ctx) -> RoutingDecision:
    # The ladder-membership guarantee is enforced as a PRECONDITION in decide(),
    # not here. Putting it in this handler made it unreachable: _r9's predicate
    # already requires a non-None subject, so a BETTER_MATCH naming a stranger
    # silently fell through to R10/R11 and produced a generic "no row matched"
    # instead of naming the actual problem. See decide().
    return RoutingDecision(
        action=DecisionAction.COMPLETE_AND_ESCALATE_PARALLEL,
        target_id=ctx.incumbent.snapshot.stakeholder.id if ctx.incumbent else None,
        escalate_to_id=ctx.subject.snapshot.stakeholder.id,
        matrix_row="R9",
        rationale=(
            f"{ctx.subject.snapshot.stakeholder.name} "
            f"({ctx.subject.score.qualification:g}) out-qualifies "
            f"{ctx.incumbent.snapshot.stakeholder.name} "
            f"({ctx.incumbent.score.qualification:g}) on a "
            f"{ctx.severity.value} alert — original completes, senior paged in "
            f"parallel. Different people, different keys, no duplicate."
        ),
    )


def _h_r11(ctx: Ctx) -> RoutingDecision:
    return RoutingDecision(
        action=DecisionAction.EXHAUSTED,
        matrix_row="R11",
        rationale="ladder exhausted with nothing committed — failing loudly",
        suppressed=tuple(ctx.state.suppressed.items()),
    )


# ─────────────────────────────────────────────────────────────────────────────
# THE MATRIX.  Order is the specification. Read alongside §3.5.
# ─────────────────────────────────────────────────────────────────────────────

MatrixRow = tuple[str, Callable[[Ctx], bool], Callable[[Ctx], RoutingDecision]]

MATRIX: tuple[MatrixRow, ...] = (
    ("R1", _r1, _continue("R1", "not the incumbent; nothing in flight to change")),
    ("R2", _r2, _h_r2),
    ("R3", _r3, _h_r3),
    ("R4", _r4, _h_r4),
    ("R5", _r5, _h_r5),
    ("R6", _r6, _h_r6),
    ("R7", _r7, _h_r7),
    ("R8", _r8, _continue("R8", "already notified — I2 outranks optimality")),
    ("R9", _r9, _h_r9),
    (
        "R10",
        _r10,
        _continue(
            "R10",
            "low severity and already correctly routed — escalation costs a "
            "human's attention",
        ),
    ),
    ("R11", _r11, _h_r11),
)

ROW_IDS: tuple[str, ...] = tuple(row_id for row_id, _, _ in MATRIX)


def decide(
    state: DispatchState,
    event: InterruptEvent,
    attempt: AttemptRecord | None,
    channels: ChannelFacts | None = None,
) -> RoutingDecision:
    """Walk the matrix top to bottom. First match wins. Pure.

    Raises NoMatchingRow when nothing matches — see the exception's docstring
    for why that is the correct behaviour rather than a default.
    """
    ctx = _build_ctx(state, event, attempt, channels or ChannelFacts())

    # PRECONDITION, not a row. BETTER_MATCH is DERIVED by the interrupt listener
    # from ladder members only, so one naming a stranger means the derivation is
    # broken — and acting on it would mean escalating to somebody we never paid
    # to evaluate, which is invariant I3. Fail here, loudly and specifically,
    # rather than letting it drift to the bottom of the table and emerge as a
    # vague "no row matched" that points at the wrong thing.
    if event.kind is InterruptKind.BETTER_MATCH and ctx.subject is None:
        raise NoMatchingRow(
            f"BETTER_MATCH names {event.stakeholder_id}, who is not in the "
            f"ladder — escalating to a non-member would require a new "
            f"availability query"
        )

    for row_id, predicate, handler in MATRIX:
        if predicate(ctx):
            decision = handler(ctx)
            assert decision.matrix_row == row_id, (
                f"row {row_id} returned a decision tagged "
                f"{decision.matrix_row!r} — the audit trail would be untraceable"
            )
            return decision
    raise NoMatchingRow(
        f"no matrix row matched {event.kind.value} for {event.stakeholder_id} "
        f"(attempt={attempt.state.value if attempt else None}, "
        f"severity={state.alert.severity.value})"
    )
