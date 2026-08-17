# Alert Routing Agent

An agent that routes operational alerts to the right stakeholder, and handles the case where
availability changes **while the notification is already being sent**.

The brief's hard requirement is a contradiction on its face: *detect mid-execution that
availability has changed*, and *do not re-query stakeholders you have already evaluated*. You
cannot notice a change you have forbidden yourself to look for. The resolution is that the
registry has **two verbs** — a counted *pull* and a free *push* that carries the new state in its
payload. The look comes to you.

---

## The four invariants

Every design decision in this repository serves one of these. Two are enforced by the database
rather than by application code, which is a materially stronger claim than "my code checks for it".

| # | Invariant | Mechanism | Test |
|---|---|---|---|
| **I1** | Context is never lost across a re-route | Frozen Pydantic models; the envelope carries the **original** `AlertEvent` by reference, plus the full audit trail | `test_context_survives_two_reroutes` |
| **I2** | Nobody is notified twice | `UNIQUE (idempotency_key)` on `dispatch_attempts`, where the key is `{alert_id}:{stakeholder_id}` and **excludes the channel** | `test_aborted_attempt_blocks_reattempt_of_same_person` |
| **I3** | One availability query per person per alert | `PRIMARY KEY (alert_id, stakeholder_id)` on `evaluations` | `test_second_pull_same_alert_raises_duplicate_query_error` |
| **I4** | Never escalate downward | `qualification` contains **no availability term**; a replacement must clear `max(severity_minimum, incumbent − tolerance)` | `test_offline_director_beats_online_junior_on_qualification` |

A second availability query is not a bug this system tries to avoid. It is a write the database
refuses:

```
sqlite3.IntegrityError: UNIQUE constraint failed:
    evaluations.alert_id, evaluations.stakeholder_id
```

---

## Setup & configuration

```bash
python -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env                                  # Windows: copy .env.example .env
```

**That is the entire setup. There is no key to provision and nothing to sign up for.**

`.env.example` ships with both values blank, and blank is a *tested* configuration — all 100+
tests, all four demo scenarios and the HTTP surface run with no credentials at all. The `cp` step
is a convenience; the code reads the environment either way.

There is no `SLACK_BOT_TOKEN` or `TWILIO_AUTH_TOKEN` in the template because **there is no Slack or
Twilio integration to hold one.** The transports are simulated: `ChannelAdapter` models connect and
send latency, persistence semantics (Slack synchronous, email and SMS persistent) and injectable
failure, then delivers to an in-memory list. That is a deliberate scope decision — it is what makes
the `failover` scenario reproducible on your machine instead of dependent on somebody's sandbox
being up — and it is listed under *What is unfinished*. A template advertising credentials the code
never reads would cost you ten minutes and tell you nothing.

The two variables below are the only two the code reads anywhere. Check it in one command:
`grep -rn "os.getenv" alert_router/`

| Variable | Required | Behaviour when blank |
|---|---|---|
| `OPENAI_API_KEY` | **no** | Explanations come from `render_template()` — the deterministic renderer the tests assert against. The audit trail labels the line `via template`. |
| `OPENAI_MODEL` | no | Ignored entirely unless a key is set. |

With a key set, the *same* text is handed to the model to rephrase. The model is **told** the
decision: it receives no candidate list, no scores, and no authority to change the recipient, the
channel or the role. Any failure — bad key, timeout, empty completion — degrades to the template
and **names the failure in the audit trail** rather than degrading silently.

---

## Run it

```bash
python -m alert_router.cli demo --scenario all        # the four scenarios, ~30 seconds
python -m alert_router.cli demo --scenario floor      # the one that refuses to re-route
python -m alert_router.cli matrix                     # the decision table, R1–R11
python -m alert_router.cli invariants                 # all four, checked against every scenario
pytest -q
```

### The HTTP surface

`POST /presence` is the push path driven from **outside** the process — which is what a Slack or
PagerDuty presence webhook actually is. Inside a test, "the event arrived" is easy to assert and
easy to disbelieve; over HTTP it is not.

```bash
uvicorn alert_router.api:app --reload
```

```bash
# An SLA breach → infrastructure
curl -X POST localhost:8000/alerts -H 'Content-Type: application/json' -d '{
  "metric_name": "db_replica_lag_seconds", "value": 94, "threshold": 30,
  "direction": "above", "severity": "critical", "domain": "infrastructure"}'

# A contract about to lapse → procurement. Note direction: it breaches by FALLING.
curl -X POST localhost:8000/alerts -H 'Content-Type: application/json' -d '{
  "metric_name": "contract_days_to_expiry", "value": 3, "threshold": 30,
  "direction": "below", "severity": "high", "domain": "procurement"}'

# An anomaly score → security
curl -X POST localhost:8000/alerts -H 'Content-Type: application/json' -d '{
  "metric_name": "auth_anomaly_score", "value": 0.94, "threshold": 0.70,
  "direction": "above", "severity": "high", "domain": "security"}'

# A stock-out → logistics
curl -X POST localhost:8000/alerts -H 'Content-Type: application/json' -d '{
  "metric_name": "warehouse_stock_units", "value": 12, "threshold": 50,
  "direction": "below", "severity": "high", "domain": "logistics"}'

# The push path: mark somebody offline from outside the process
curl -X POST localhost:8000/presence -H 'Content-Type: application/json' -d '{
  "stakeholder_id": "stk-001", "status": "offline", "reason": "laptop closed"}'

# Read the incident narrative back, ordered and gapless
curl localhost:8000/alerts/<alert_id>/audit
```

The brief names four metric families — stock levels, contract expiry, SLA breaches, anomaly
scores. The registry has an owner for each; the CLI demo exercises two of them and
`tests/test_api.py::test_routing_is_domain_agnostic` exercises the other two. Nothing in the
router knows what a metric *means*: `domain` is a routing key, not a branch.

---

## What the demo shows

Four scenarios, each on its own clean database, each demonstrating one row of the decision matrix.

| Scenario | Row | What happens |
|---|---|---|
| `reroute` | **R3** | Priya drops offline mid-Slack. Abort inside the window, re-route to Tom (123 ≥ floor 120). Query count unchanged. |
| `floor` | **R4** | Same failure, but Tom is off-rota at 108. **The agent refuses to route down** — holds Priya on SMS, escalates to Elena, records the arithmetic. |
| `escalate` | **R9** | Elena comes online mid-dispatch at 140 > 139. Priya's send completes; Elena is paged in parallel. Two people, zero duplicates. |
| `failover` | **R6** | Slack refuses. Same person, same idempotency key, next pipe. Also a `below`-threshold depletion breach. |

The `floor` scenario is the interesting one. Every alert router can re-route; this one **declines
to**, and says why:

```
Tom Beckett   qualification 108 < floor 120
              (max of CRITICAL minimum 120, incumbent Priya Raman 139 − tolerance 25)
```

---

## Architecture

```
  AlertEvent  ──▶  validate (must actually breach)  ──▶  persist
       │
       ▼
  RESOLVE ......... ONE indexed SELECT (primary_domain OR json_each(secondary))
       │            matched people → INSERT evaluations        ← I3 lives here
       ▼
  RANK ............ qualification ⟂ reachability, frozen into a tuple
       │
       ▼
  RESERVE ......... INSERT dispatch_attempts(state='reserved')  ← I2 lives here
       │            committed BEFORE sending, which is why an ABORTED
       │            attempt still blocks a retry
       ▼
  SEND ............ connecting → in_flight     ┌── PresenceBus ──┐
       │            (the abort window)          │ push events     │
       │  ◀───────── interrupt ─────────────────┘                 │
       │            patch_from_push: new knowledge, no query       │
       │            decision matrix R1–R11: first match wins       │
       ▼
  COMMIT .......... asyncio.shield() guards this write — one-way door
       │
       ▼
  RENDER .......... original alert + audit trail + numeric "why you"
```

### Where this system starts and stops

**It does not poll metrics, and that is a decision rather than an omission.**

The brief opens with "monitors operational metrics" and then specifies, in the requirements,
"accept an alert event (metric name, value, threshold, severity)". Those are two different
systems. Threshold evaluation over a time series is a solved, well-served problem — Prometheus
Alertmanager, Datadog monitors, CloudWatch alarms — and rebuilding a worse one would have
consumed the time the interesting half needs.

So the seam is `POST /alerts`, shaped exactly like the webhook those tools already emit. Upstream
owns *when* a threshold is crossed. This system owns everything after: who should hear about it,
which pipe reaches them, and what to do when the answer changes while the message is already in
flight. That is where the brief's actual difficulty lives — the mid-flight interrupt, the
no-double-query constraint, the refusal to route downward — and it is where all of the
engineering went.

Two consequences worth stating plainly, because they are the cost of the choice:

- The system is **edge-triggered, not level-triggered.** It reacts to a breach it is told about;
  it will not notice a metric that is still breaching an hour later. A real deployment pairs it
  with Alertmanager's `repeat_interval`.
- `AlertEvent` still validates the breach itself — `value` must actually cross `threshold` in the
  stated `direction`, or the request is a 422. Trusting the caller's arithmetic would make a
  misconfigured upstream look like a routing bug.

### Pull and push

`registry.query_by_domain()` is the **pull**: one indexed SELECT, simulated latency, and an
`evaluations` row per matched person. The composite primary key means a second pull for the same
person on the same alert raises `IntegrityError`.

`PresenceBus` is the **push**: free, uncounted, and the event payload carries `previous` and
`current`. `DispatchState.patch_from_push()` updates the cached snapshot from the payload. No
question is asked, so nothing is charged.

**The query counter measures questions asked, not facts known.** You can watch it stay flat while
the routing decision changes.

### Why the snapshot must go stale

`CandidateSnapshot` is a point-in-time observation that never refreshes itself. That staleness is
the feature: it is what allows a divergence to exist between what the agent believes and what is
true, and *that divergence is the mid-flight signal*. If snapshots tracked the database there
would be no change left to detect.

### Qualification and availability are separate axes

```
qualification = DOMAIN_POINTS[fit] + 8 × seniority_tier + (15 if on_call)
sort_key      = (reachability, qualification, seniority_tier)   descending
```

There is no availability term in `qualification`. Because presence is not an addend, no amount of
being online can raise a junior above a senior, and nobody can demote themselves by stepping away
from their desk.

The consequence looks wrong at a glance and is correct:

```
rank  name             fit         tier  status   reach  QUAL
0     Priya Raman      primary     L3    online   2       139
1     Tom Beckett      primary     L1    online   2       123
...
6     Elena Fischer    primary     L5    offline  0       140   ← last, and the most qualified
```

Elena sorts last because an unreachable person cannot answer a synchronous page. She is still the
most qualified person in the list, and every *replacement* comparison uses qualification alone.
Ordering for first contact and ranking for competence are different questions.

### The commit point

Dispatch is `reserved → connecting → in_flight → committed`. Everything before the commit is
abortable and the abort is clean, because nothing has left the building. `asyncio.shield()` appears
twice, and **neither instance protects the send**:

- around the **commit** write, so a delivered message is always recorded as delivered;
- around the **abort** write, because that cleanup runs inside an already-cancelled task and an
  unshielded `await` there would itself be cancelled — leaving the row stuck at `in_flight` and the
  audit trail silent about an abort that definitely happened.

One makes success durable, the other makes failure durable.

`executor.request_abort()` returns `False` once the commit point has passed. That boolean *is* the
abort window, exposed as an API, and it is what row R5 keys off.

---

## The decision matrix

Evaluated top to bottom, **first match wins**. Implemented as a literal ordered sequence of
`(row_id, predicate, handler)` tuples, not nested conditionals — nesting is how one row silently
shadows another. Every `RoutingDecision` carries its `matrix_row`, so the audit trail is traceable
back to this table.

| # | Interrupt | Phase | Condition | Decision |
|---|---|---|---|---|
| R1 | any | any | not the incumbent | `CONTINUE_UNCHANGED` — cross-domain noise never wakes the matrix |
| R2 | presence → offline | pre-commit | channel is persistent | `CONTINUE_UNCHANGED` — SMS/email wait on the device |
| R3 | presence → offline | pre-commit | synchronous channel ∧ a candidate clears the floor | `ABORT_AND_REROUTE` |
| R4 | presence → offline | pre-commit | synchronous ∧ **nobody** clears the floor | `HOLD_AND_ESCALATE_UP` — **this row is I4** |
| R5 | presence → offline | **post-commit** | — | `COMPLETE_AND_ESCALATE_PARALLEL` — you cannot unsend a message |
| R6 | channel degraded | pre-commit | a healthy fallback exists | `CHANNEL_FAILOVER` — same person, same key |
| R7 | channel degraded | pre-commit | no healthy channel remains | `ABORT_AND_REROUTE` |
| R8 | better match | any | already notified | `CONTINUE_UNCHANGED` — I2 outranks optimality |
| R9 | better match | any | higher qualification ∧ severity ≥ HIGH | `COMPLETE_AND_ESCALATE_PARALLEL` |
| R10 | better match | any | severity = LOW | `CONTINUE_UNCHANGED` — escalation costs a human's attention |
| R11 | any | any | ladder exhausted, nothing committed | `EXHAUSTED` — terminal, loud, recorded |

R2 and R5 fire on the identical event and differ **only** by phase. An event matching no row
**raises** — a silent default would make every future gap in this table invisible.

---

## Design decisions I would defend

**Two verbs, not one.** A pull is a question; a push is an announcement. Only questions are
counted. This is also how real presence works — Slack and PagerDuty deliver it as a webhook, not a
poll.

**The idempotency key excludes the channel.** A failover from Slack to SMS is the same notification
down a different pipe, so it must collide with itself. `executor.failover()` therefore UPDATEs the
existing attempt row rather than creating a second one — calling `dispatch()` twice for the same
person raises `DuplicateDispatchError`, which is the schema working correctly.

**The key is taken at reservation, not at delivery.** That is why an aborted attempt still blocks a
retry: the row exists from the moment we commit to trying.

**SQLite rather than JSON.** The brief permitted either. A database was chosen because two
invariants become schema constraints, which costs about twenty lines and converts a promise into a
property.

**No Celery, no LangGraph.** A broker moves the work to another process and puts the cancellation
window out of reach — and that window is the assessment. The decision logic is a deterministic
eleven-row truth table, not a reasoning loop; wrapping it in an LLM graph would make it slower,
non-deterministic and impossible to test exhaustively.

**Channel persistence is a first-class property.** Email and SMS wait on the device; Slack does
not. "Offline" means away from keyboard, not unreachable. Conflating the two causes a whole class
of pointless re-routes, which is why a presence drop mid-SMS is a non-event (R2) and the same drop
mid-Slack is a real problem (R3).

**The LLM renders prose and nothing else.** No tools, no candidate list, no routing authority, and
a deterministic fallback. If the routing decision could vary with model output, the truth table
would stop meaning anything.

**Failing loudly.** An unmatched interrupt raises. An exhausted ladder terminates as `exhausted`,
recorded. A silent drop is the worst possible outcome for an alerting system, so the one case that
must never be quiet is the one where nobody was reached.

---

## What is unfinished

Stated plainly, because knowing what you did not build is part of the work.

- **Crash recovery.** A process death between `reserved` and `committed` leaves an orphaned row and
  the alert stalls. The fix is a sweeper that ages reserved rows out; I chose to ship four working
  scenarios instead.
- **`POST /presence` is unauthenticated.** Anyone who can reach the process can mark anyone offline
  and steer the routing. Correct for a demo, indefensible in production.
- **Single-process only.** `PresenceBus` is in-memory, so two workers would each hold their own view
  of who is available.
- **No acknowledgement or de-escalation.** Nobody acks, so nothing stands down.
- **Transports are simulated.** Channel adapters have injectable latency and failure, which is what
  makes the scenarios deterministic — but nothing actually sends a message.
- **`on_call` is a boolean, not a rota.** No time zones, no shift handover.
- **No rate limiting on escalation storms.** A flapping stakeholder could generate repeated
  `BETTER_MATCH` derivations; they are deduplicated per person, not per unit time.
- "No cross-alert coordination. Three simultaneous breaches in one domain can all route to the same person;
   load balancing needs a global claim, which is a Postgres SKIP LOCKED change rather than a SQLite one.  

---

## Next steps with more time

- Redis pub/sub for presence, so the push channel crosses process boundaries.
- Postgres with `SKIP LOCKED` for multi-worker dispatch; the interfaces are already shaped for it.
- A learned ranker over historical response times, replacing the hand-tuned weights in `config.py`.
- OpenTelemetry spans per attempt, so the abort window is visible in a trace rather than only in
  the audit trail.
- Crash recovery: sweep `reserved` rows older than a threshold and either resume or fail them.

---

## Repository layout

```
alert_router/
  config.py       all tunables and the enumerated values shared with the schema
  models_orm.py   SQLAlchemy 2.0 tables — the constraints are the specification
  db.py           async engine, per-connection PRAGMA listener, seeding
  schemas.py      frozen Pydantic contracts
  registry.py     PULL (counted) + PUSH (PresenceBus)
  ranking.py      scoring and the frozen ladder — pure, no database
  state.py        DispatchState: four disjoint ledgers, one lock, the audit trail
  channels.py     transport adapters and persistence classification
  executor.py     two-phase send, the commit point, failover
  interrupts.py   listener, relevance filter, BETTER_MATCH derivation
  decisions.py    the decision matrix R1–R11 — pure, no I/O
  context.py      envelope compilation, template and LLM renderers
  agent.py        orchestration: TaskGroup, ladder walk, decision application
  scenarios.py    the four demo scenarios, defined once
  cli.py          Typer + Rich demo
  api.py          FastAPI surface
tests/
  test_registry.py     I3 at the data layer
  test_ranking.py      I4 — qualification ⟂ availability
  test_executor.py     commit point, abort window, the shields
  test_decisions.py    the full truth table, no database
  test_context.py      I1 across two re-routes
  test_invariants.py   all four, end to end, every scenario
  test_api.py          the HTTP surface, incl. the push path from outside the process
verify_module1..5.py   per-module verification scripts, kept as evidence
docs/                  the architecture plan this was built from
```

`verify_module*.py` are kept deliberately: each one prints the evidence for its module's gate, and
together they are the record of how the system was checked as it was built.
