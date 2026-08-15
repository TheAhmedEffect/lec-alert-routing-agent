"""
Scoring and ladder construction.

THIS MODULE IS PURE. No session, no await, no database — snapshots in, a frozen
DispatchPlan out. That is not tidiness for its own sake: if build_ladder() wrote
its own ladder_rank values, every ranking test would need an engine, and the
scoring rules — the part a reviewer will actually interrogate — would become
untestable in isolation. The write-back lives in state.persist_ladder().

INVARIANT I4 LIVES IN ONE LINE OF THIS FILE
-------------------------------------------
    qualification = domain_points + seniority_points + on_call_points

There is no availability term. None. That absence is the whole mechanism: because
presence is not an addend, no amount of being online can raise a junior above a
senior, and nobody can demote themselves by stepping away from their desk.

Availability enters exactly once, as `reachability`, and only as the FIRST key of
the sort — because an unreachable person cannot be the *initial* recipient, so
there is no point ranking them there. Every *replacement* comparison
(clears_floor) uses qualification alone.

The consequence is worth stating plainly, because it looks wrong at a glance:
Elena Fischer is the most qualified infrastructure responder in the registry
(140) and she sorts LAST, because she is offline. Priya (139, online) leads. That
is correct. Ordering for first contact and ranking for competence are different
questions, and conflating them is exactly the bug the brief warns about.
"""

from __future__ import annotations

import time
from typing import Callable, Iterable, Sequence

from . import config
from .schemas import (
    AlertEvent,
    CandidateSnapshot,
    DispatchPlan,
    RankedCandidate,
    ScoreBreakdown,
    Severity,
)

Clock = Callable[[], float]


# ─────────────────────────────────────────────────────────────────────────────
# Scoring
# ─────────────────────────────────────────────────────────────────────────────


def score(snapshot: CandidateSnapshot, alert: AlertEvent) -> ScoreBreakdown:
    """Score one observation against one alert.

    Every term is recorded separately so the final notification can say
    "chosen over Marcus because primary domain 100 versus secondary 55" with
    real arithmetic rather than an assertion of fitness.

    A domain_fit of "none" means INELIGIBLE, not zero points. Someone with no
    overlap at all is not a weak candidate; they are not a candidate.
    """
    person = snapshot.stakeholder
    fit = person.domain_fit(alert.domain)
    domain_points = config.DOMAIN_POINTS[fit]

    # Reachability is read from the SNAPSHOT's status, not the person record's.
    # After a push patch those differ, and the snapshot is what we are entitled
    # to know — the person record inside it is the stale pull-time copy.
    reachability = snapshot.status.reachability

    if domain_points is None:
        return ScoreBreakdown(
            domain_points=0.0,
            seniority_points=0.0,
            on_call_points=0.0,
            qualification=0.0,
            reachability=reachability,
            eligible=False,
            notes=(f"no overlap with domain '{alert.domain}'",),
        )

    seniority_points = config.SENIORITY_POINTS * person.seniority_tier
    on_call_points = config.ON_CALL_POINTS if person.on_call else 0.0

    # ── INVARIANT I4 ──────────────────────────────────────────────────────
    # Three terms. Availability is not one of them and must never become one.
    # If you are here to "just add a small bonus for being online", stop: that
    # single line is the failure mode the assessment is built around.
    qualification = domain_points + seniority_points + on_call_points
    # ──────────────────────────────────────────────────────────────────────

    notes = [f"{fit} domain +{domain_points:g}",
             f"L{person.seniority_tier} +{seniority_points:g}"]
    if person.on_call:
        notes.append(f"on-call +{on_call_points:g}")

    return ScoreBreakdown(
        domain_points=domain_points,
        seniority_points=seniority_points,
        on_call_points=on_call_points,
        qualification=qualification,
        reachability=reachability,
        eligible=True,
        notes=tuple(notes),
    )


# ─────────────────────────────────────────────────────────────────────────────
# The ladder
# ─────────────────────────────────────────────────────────────────────────────


def sort_key(candidate: RankedCandidate) -> tuple[int, float, int]:
    """(reachability, qualification, seniority_tier), sorted descending.

    Reachability leads because the ladder's job is to answer "who do we contact
    FIRST", and an unreachable person cannot answer a synchronous page. It does
    NOT mean they are less qualified — see clears_floor(), which ignores
    reachability entirely.
    """
    return (
        candidate.score.reachability,
        candidate.score.qualification,
        candidate.snapshot.stakeholder.seniority_tier,
    )


def build_ladder(
    snapshots: Iterable[CandidateSnapshot],
    alert: AlertEvent,
    *,
    clock: Clock = time.time,
    plan_version: int = 1,
) -> DispatchPlan:
    """Score, filter, sort, freeze.

    The returned plan's ladder is a TUPLE and the plan is frozen. Membership
    never grows again without a new pull, which is what makes invariant I3
    structural rather than aspirational: you cannot route to someone you never
    paid to learn about, because there is nowhere to put them.
    """
    scored = [
        RankedCandidate(snapshot=snap, score=score(snap, alert), rank=0)
        for snap in snapshots
    ]
    eligible = [candidate for candidate in scored if candidate.score.eligible]
    eligible.sort(key=sort_key, reverse=True)

    ranked = tuple(
        candidate.model_copy(update={"rank": index})
        for index, candidate in enumerate(eligible)
    )
    return DispatchPlan(
        alert=alert, ladder=ranked, created_at=clock(), plan_version=plan_version
    )


# ─────────────────────────────────────────────────────────────────────────────
# The floor — invariant I4's enforcement point
# ─────────────────────────────────────────────────────────────────────────────


def floor_for(incumbent_qualification: float | None, severity: Severity) -> float:
    """The minimum qualification a replacement must reach.

    Two constraints, whichever binds harder:

      * the severity minimum — a CRITICAL alert deserves a competent responder
        regardless of who was originally chosen;
      * the incumbent minus a tolerance — a replacement may be slightly less
        qualified than the person they replace (people are not interchangeable
        and some slack is realistic), but not arbitrarily less.

    DOWNGRADE_TOLERANCE is roughly one seniority step plus on-call duty. Wider
    and the floor stops meaning anything; narrower and legitimate re-routes get
    refused.
    """
    minimum = config.MIN_QUALIFICATION[severity.value]
    if incumbent_qualification is None:
        return minimum
    return max(minimum, incumbent_qualification - config.DOWNGRADE_TOLERANCE)


def clears_floor(
    candidate: RankedCandidate,
    incumbent: RankedCandidate | None,
    severity: Severity,
) -> tuple[bool, str]:
    """Whether `candidate` may replace `incumbent`, and WHY, with numbers.

    NOTE WHAT IS ABSENT: reachability plays no part here. This comparison is
    purely about competence, which is why an online junior cannot displace an
    offline senior no matter how convenient that would be.

    The returned string goes straight into the audit trail, into the
    notification, and onto the screen in the walkthrough video. "not qualified"
    is worth nothing to a reviewer; the arithmetic is the entire argument.
    """
    minimum = config.MIN_QUALIFICATION[severity.value]
    incumbent_qualification = incumbent.score.qualification if incumbent else None
    floor = floor_for(incumbent_qualification, severity)
    candidate_qualification = candidate.score.qualification
    name = candidate.snapshot.stakeholder.name

    if not candidate.score.eligible:
        # No domain overlap at all — not a weak candidate, not a candidate.
        # This function does not receive the alert, so it cannot name the
        # domain; score() already recorded it in ScoreBreakdown.notes.
        return False, f"{name} is ineligible: no domain overlap"

    if incumbent is None:
        basis = f"{severity.value.upper()} minimum {minimum:g}"
    else:
        basis = (
            f"max of {severity.value.upper()} minimum {minimum:g}, "
            f"incumbent {incumbent.snapshot.stakeholder.name} "
            f"{incumbent_qualification:g} - tolerance {config.DOWNGRADE_TOLERANCE:g}"
        )

    if candidate_qualification >= floor:
        return True, (
            f"{name} qualification {candidate_qualification:g} >= floor {floor:g} ({basis})"
        )
    return False, (
        f"{name} qualification {candidate_qualification:g} < floor {floor:g} ({basis})"
    )


def best_by_qualification(
    candidates: Sequence[RankedCandidate],
) -> RankedCandidate | None:
    """The most QUALIFIED candidate, ignoring availability entirely.

    Used by decision-matrix row R4, which escalates UP when nobody reachable
    clears the floor. Deliberately not `ladder[0]` — that is the most
    CONTACTABLE candidate, which is a different person and a different question.
    """
    if not candidates:
        return None
    return max(candidates, key=lambda c: (c.score.qualification,
                                          c.snapshot.stakeholder.seniority_tier))
