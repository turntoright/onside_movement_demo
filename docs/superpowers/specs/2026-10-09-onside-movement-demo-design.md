# Onside movement demo — design

Date: 2026-10-09. Revision 2: blocking issues from the first independent review fixed
(see §12).

## 1. Purpose

A portfolio demo for the Onside Data Engineer process (next technical round). It shows
the JD stack applied to Onside's own domain: farm check-ins, movements between
properties, and biosecurity stand-down rules.

Success means:
- A 5-minute live demo runs offline on a laptop (LocalStack).
- The same Terraform deploys once to real AWS, and screenshots prove it.
- A public GitHub repo whose README maps each part to an Onside product feature.
- The story joins the two halves of the JD: event-driven AWS ingestion and
  warehouse model quality.

Domain facts come from the public Onside help centre and from arXiv 2602.21491
(co-authored by Onside). Full notes: `mycv_management/opportunities/2026-09-onside-data-engineer/movement-data-research.md`.

### In scope
- AWS event chain: EventBridge, SQS standard queue + DLQ, Lambda, DynamoDB, Firehose,
  S3.
- Terraform for all infrastructure, one codebase for LocalStack and AWS.
- Dagster orchestration with dagster-dbt.
- dbt with the DuckDB adapter, reading S3.
- Synthetic data generator.

### Out of scope (named in the README as "production next steps")
- API Gateway / webhook ingestion (the generator plays the app backend).
- ClickHouse, Iceberg, Athena, BI dashboards.
- Incremental dbt models (full rebuild at demo scale).
- The real product's rules between individual properties and groups, and temporary
  outbreak overrides. The demo models rules between **property types** only.

## 2. Architecture

```
 Generator (Python)
   ├─ setup: reference data → DynamoDB `properties` + S3 ref/ (same snapshot)
   └─ PutEvents in received_time order: CheckIn / CheckOut / PlantMovement
        ▼
 EventBridge bus: onside-events
   ├─ rule (source=onside.app, CheckIn|CheckOut)
   │     ──► SQS visit-queue (standard, DLQ after 3 receives)
   │           ──► Lambda standdown_checker
   │                 ├─ DynamoDB properties            (read, cached per container)
   │                 ├─ DynamoDB person_state          (one item per person, versioned)
   │                 └─ after commit: PutEvents (source=onside.standdown)
   │                       CheckInEvaluated (every check-in)
   │                       HighRiskVisitDetected (breaches only)
   └─ rule (all sources) ──► Firehose (input transformer: one event per line)
                              ──► S3 raw/ (arrival-hour prefixes)
                                    │
 Dagster (schedule + S3 sensor)
   └─ dbt-duckdb reads S3 → models → tests → DuckDB file
        └─ outputs: movements, network, high-risk visits, reconciliation,
                    key properties; trace job
```

The SQS rule filters on `source = onside.app`, so the Lambda's own events never loop
back into the queue.

### Components

| Unit | Responsibility | Depends on |
|---|---|---|
| `generator/` | Builds reference data and an event stream; loads reference data; emits events; writes a manifest; can also write the same JSON to local files | boto3 |
| `lambdas/standdown_checker/rules.py` | Pure stand-down logic (§4): given a check-in, the person's visits and the rule matrix, return the evaluation | stdlib only |
| `lambdas/standdown_checker/state.py` | Read and conditionally write `person_state`; trim the window | boto3 |
| `lambdas/standdown_checker/handler.py` | SQS batch handler (§6.1) | rules, state |
| `shared/standdown_rules.csv` | The single source of rules: bundled into the Lambda zip and used as a dbt seed | — |
| `shared/golden_cases.csv` | Shared edge cases, run by both pytest and dbt (§7) | — |
| `infra/terraform/` | All AWS resources; a variable switches between LocalStack endpoints and real AWS | — |
| `dbt/` | Staging → intermediate → marts, tests, a Python model for network metrics | DuckDB, S3 |
| `orchestration/` | Dagster definitions: dbt assets, S3 sensor, asset checks, trace job | dbt, boto3 |

Validation uses stdlib dataclasses, not pydantic, so the Lambda zip has no compiled
dependencies.

## 3. Data and time

### 3.1 Time model
- Every timestamp is ISO 8601 with a UTC offset. "Local" always means
  `Pacific/Auckland`.
- `event_time` is when it happened on the device. `received_time` is when the backend
  received it. The generator fixes both.
- The generator emits events **in `received_time` order**. Two modes:
  - `generate --backfill`: replays 12 months as fast as LocalStack accepts them, in
    PutEvents batches of 10. It runs once before the demo and takes tens of minutes.
  - `generate --live N`: emits N events with times shifted so that the last
    `received_time` is about now. This is the live part of the demo.
- The Lambda never uses its own clock for business logic:
  - `detected_time` (on its events) = the `received_time` of the check-in being
    evaluated.
  - Wall-clock time is kept separately as `processed_at`, for operations only.
- **Window watermark:** `person_state.watermark` is the largest `event_time` seen for
  that person. Trimming uses it (§6.1), never `now()`.

### 3.2 Generator behaviour
- Fixed random seed, so output is reproducible. Default scale: about 500 properties,
  2,000 people, 100,000 check-ins over 12 months.
- Properties have a type (e.g. poultry, pork, beef, dairy, kiwifruit, vineyard, office,
  feed mill), group, owning company, region and coordinates. The reference data is
  static for the whole run (no history of changes).
- About 70% of moves between properties are under 10 km, with rare long moves (from
  the paper's 71.3%).
- Offline: about 5% of check-ins and check-outs arrive late, with
  `received_time − event_time` up to **7 days** (a hard maximum).
- Duplicates: about 2% of events are sent again with the **same event_id and body**,
  at most 7 days after the original.
- Every CheckOut belongs to a CheckIn that exists. There is **at most one** CheckOut
  (one distinct event_id) per check-in. Check-out time ≥ check-in time.
- Some visits have no check-out; downstream infers one (§5).
- A person's visits do not overlap: each check-in comes after the previous visit's
  check-out.
- `additional_visitors` is an integer count (group check-in, "+N"). It is reported but
  never expanded into movements or stand-down checks.
- It emits only valid events on the normal path. `generate --poison K` sends K
  malformed events with `detail.poison = true` to demonstrate the DLQ.
- It writes `manifest.json`: mode, seed, and distinct event_id counts per event type.
- `--local-out DIR` writes the same events as JSON lines, so dbt can be built and
  tested without AWS.

### 3.3 Events (`detail` of each EventBridge event)

| Event | Source | Fields |
|---|---|---|
| `CheckIn` | onside.app | event_id, person_id, company_id, property_id, event_time, received_time, method (app, geofence, qr, kiosk, link), job_type, additional_visitors, answers[] |
| `CheckOut` | onside.app | event_id, checkin_event_id, person_id, property_id, event_time, received_time, method (manual, geofence_auto, end_of_day) |
| `PlantMovement` | onside.app | event_id, source_property_id, dest_property_id, movement_date (local date), material_type, volume, received_time |
| `CheckInEvaluated` | onside.standdown | event_id = hash("eval", checkin_event_id), checkin_event_id, person_id, detected_time, processed_at, origins_seen[] (origin checkin_event_id, end, end_source), breach (bool), rule_id, origin_checkin_event_id |
| `HighRiskVisitDetected` | onside.standdown | event_id = hash("alert", checkin_event_id), checkin_event_id, person_id, origin_property_id, dest_property_id, origin_checkin_event_id, nights_between, required_nights, rule_id, detected_time, processed_at |

PlantMovement does not go to the Lambda. It is used only by tracing (§5).

### 3.4 Reference data
- The `properties` snapshot (property_id, type, group, company_id, region, lat, lon) is
  written once, at setup, to both DynamoDB `properties` and S3 `ref/properties.json`.
- People and companies are written to S3 `ref/` only.
- `shared/standdown_rules.csv` has columns rule_id, from_type, to_type, scope
  (internal, external, any) and nights.
  - (from_type, to_type, scope) is unique.
  - 1 ≤ nights ≤ 14.

## 4. Stand-down logic (identical in Lambda and dbt)

Notation: D is the new check-in. An **origin candidate** O is another visit by the same
person with an observed check-in, where:
- (O.checkin_time, O.checkin_event_id) < (D.checkin_time, D.checkin_event_id), and
- O.property_id ≠ D.property_id.

1. **End of a visit for stand-down:**
   `end(O) = greatest(O.checkin_time, earliest check-out of O)`.
   - If no check-out is known, use `O.checkin_time`.
   - Inferred check-outs (§5) are **never** used here.
2. **Nights:**
   `nights_between(O, D) = max(0, local_date(D.checkin_time) − local_date(end(O)))`,
   as an integer, using local dates. Daylight-saving days need no special handling.
3. **Window:** O counts only if `nights_between(O, D) ≤ 14`. Because every rule has
   nights ≤ 14 and a breach needs `nights_between < nights`, the window never changes
   the result. It only bounds the state.
4. **Scope:** internal if O and D are owned by the same company, otherwise external.
5. **Rule for one (O, D) pair:** among the rules with from_type = O.type and
   to_type = D.type, the **most specific** scope wins (the matching internal or
   external rule beats `any`). If no rule matches, the pair has no constraint.
6. **Breach of a pair:** `nights_between(O, D) < rule.nights`.
7. **Result for D:** D is a breach if any pair breaches. The reported (origin, rule) is
   picked from the breaching pairs by:
   - largest `rule.nights`, then
   - largest shortfall (`nights − nights_between`), then
   - latest `end(O)`, then
   - largest `O.checkin_event_id`.
8. **Grain:** exactly one result per D.checkin_event_id.

Worked example: poultry A on the 3rd, office B on the 4th, poultry C on the 4th. The
movement (§5) is B→C, but A→C has 1 night against a 3-night rule, so C is a breach.

**Why realtime can only miss, never false-alert.** The rule is monotone:
- adding a visit can only add breaching pairs;
- a later `end(O)` can only reduce nights.

The Lambda always sees a subset of the visits that batch sees, and for each one an
`end` that is ≤ batch's end. A check-out not yet seen gives `end = checkin_time`, which
is ≤ the real end. Because there is at most one distinct check-out per check-in (§3.2),
"earliest seen" is the same as "earliest overall". So any breach that realtime finds,
batch also finds. This needs §3.2's guarantees and the check-out uniqueness test
(§6.3).

## 5. dbt models

### Staging
- `stg_checkins`, `stg_checkouts`, `stg_plant_movements`, `stg_evaluations`,
  `stg_realtime_alerts`.
- Read raw objects with `read_json(..., format='newline_delimited')` over an explicit
  glob, from S3 or from `--local-out`, chosen by the target.
- Deduplicate: events on event_id; evaluations and alerts on checkin_event_id.
- Cast types and add local times.

### Intermediate
`int_visits` has one row per check-in. Columns:
- `standdown_end`: as defined in §4.
- `visit_end` and `checkout_source`. If there is a real check-out, use it
  (`checkout_source = actual`). If not, infer it as the earlier of:
  - the person's next check-in, and
  - 23:59:59 local on the check-in's own local date.

  An inferred check-out sets `checkout_source = inferred_next_checkin` or
  `inferred_end_of_day`.

### Marts
- **`fct_high_risk_visits`:** the §4 result, one row per breaching checkin_event_id.
- **`fct_movements`:** consecutive visits by the same person, ordered by
  (checkin_time, checkin_event_id), at different properties (no self-loops).
  - Timing uses `visit_end` (inferred allowed). `gap_hours = D.checkin − O.visit_end`,
    clipped at 0. `nights_between` is defined as in §4, but using `visit_end`.
  - Also: distance_km (haversine), internal/external, month.
  - `connected` is true when `gap_hours < 24 × var('connected_window_days')`
    (default 7).
- **`fct_alert_reconciliation`:** see §6.2.
- **`fct_network_edges_monthly`:** origin → destination per month. The weight is the
  average daily movements, as in the paper.
- **`dim_property_network`** (dbt Python model, networkx): built from **all 12 months**
  of people movements, with edge weight = movement count. Columns: in-degree,
  out-degree, weighted strength, weighted PageRank. This is the demo's "key property
  identification".

### Tracing
A Dagster job with config property_id, start_time and max_hops (default 3).
- **Output:** for each reachable property, its **earliest arrival time**, its hop
  count, and one witness path. Paths are never enumerated.
- **Edges:**
  - **People:** `connected` movements. Departure = the origin `visit_end`
    (inferred allowed). Arrival = the destination check-in.
  - **Plant material:** departure = start of `movement_date` (local), arrival = end of
    `movement_date`.
- **A hop is valid** when `departure ≥ arrival_time(origin)`. The seed property's
  arrival time is `start_time`.
- **Algorithm:** up to `max_hops` rounds in DuckDB.
  - Each round relaxes the edges out of the properties whose arrival improved in the
    previous round.
  - It keeps the minimum arrival per property, with the predecessor as the witness.
  - It stops early when nothing improves.

All models are rebuilt in full. The README explains the production alternative:
incremental by received_time, a lookback window, and recomputing the affected people.

## 6. Error handling and data quality

### 6.1 Lambda `standdown_checker`
- **Queue:** SQS standard queue with partial batch responses (`ReportBatchItemFailures`).
  Delivery order is **not** assumed: EventBridge does not guarantee it, so the handler
  is order-independent.
- **State:** one `person_state` item per person, holding:
  - `version`
  - `watermark`
  - `visits[]`, each with checkin_event_id, property_id, type, company_id,
    checkin_time, checkout_event_id, checkout_time and evaluation.
  - A placeholder visit has no check-in yet (see CheckOut below).
- **Processing one record:**
  1. Validate. If invalid, the record fails. After 3 receives it moves to the DLQ.
     Only `poison` events should ever reach the DLQ.
  2. Load the person item (with `version`).
  3. **CheckIn:**
     - If its event_id is already in `visits[]`: it is a duplicate or a redelivery.
       Go to step 6 and re-emit the stored evaluation.
     - Otherwise look up the property type and company (cached), evaluate per §4 against
       the non-placeholder visits, and insert the visit at its sorted position.
     - If a placeholder with the same checkin_event_id exists, merge into it.
  4. **CheckOut:** find the visit by `checkin_event_id`.
     - If none exists, create a placeholder visit holding only the check-out.
       Placeholders are never origins (§4 requires an observed check-in).
     - If the visit already has a check-out, keep the earlier one.
     - Re-receiving the same event_id changes nothing.
  5. Update the watermark, then trim. Drop visits whose local end date is more than
     **22 nights** before the watermark: 14 for the window, plus 7 for the maximum
     lateness, plus 1 for safety. **Commit** with one conditional write
     (`version = :expected`).
     - On a version conflict, reload and retry, up to 3 times. After that, fail the
       record so it is redelivered.
     - Recording a processed event and committing are the same write, so a crash
       before the commit just means a redelivery.
  6. **After the commit**, call PutEvents with `CheckInEvaluated` and, for a breach,
     `HighRiskVisitDetected`.
     - Their event_ids are deterministic, so re-emitting is harmless; staging
       deduplicates on checkin_event_id.
     - If PutEvents fails, the record fails. The redelivery hits the duplicate path in
       step 3 and re-emits.
- **Duplicates arrive within the retention window.** The generator guarantees a gap of
  at most 7 days, which is less than the 22 nights kept. So the dedupe in step 3 always
  finds the original.

### 6.2 Reconciliation (`fct_alert_reconciliation`)

One row per checkin_event_id that is a breach on either side, joined to
`stg_evaluations`.

| Class | Rule | Expectation |
|---|---|---|
| `matched` | Both sides breach. `same_attribution` compares rule_id and origin, for information. `late_alert = (received_time − event_time > 1 h)` for D. | Most rows |
| `missed` / `origin_not_seen` | Batch's origin is not in the evaluation's `origins_seen`. `origin_was_late` says whether that origin arrived after D was evaluated. | Some |
| `missed` / `origin_end_not_seen` | Batch's origin was seen, but with an earlier end, because its CheckOut wasn't seen yet. | Few |
| `missed` / `not_evaluated` | No evaluation exists for D. | **Must be 0** |
| `missed` / `unexplained` | Anything else. | **Must be 0** |
| `false_alert` | Realtime breaches, batch doesn't. | **Must be 0** |

### 6.3 Tests
- **Generic dbt tests:**
  - unique and not-null on event_id in staging;
  - relationships for property_id and person_id;
  - accepted values for method and type;
  - (from_type, to_type, scope) unique in the rules seed.
- **Singular dbt tests:**
  - at most one distinct check-out per checkin_event_id;
  - check-out ≥ check-in;
  - movement origin ≠ destination;
  - every rule has 1 ≤ nights ≤ 14;
  - no evaluated check-in has an origin outside the 14-night window that breaches.
- **Golden cases:** `shared/golden_cases.csv` is loaded as a seed. A singular test runs
  the dbt nights and stand-down macros over it and compares with the expected column.
  Pytest runs `rules.py` over the same file.
- **dbt unit tests** (SQL models): the inferred check-out in `int_visits`, the
  stand-down pick in `fct_high_risk_visits`, and the reconciliation classes.
- **Reconciliation:**
  - `false_alert`, `not_evaluated` and `unexplained` = 0, at error level;
  - the missed rate above a threshold, at warn level.

### 6.4 Dagster asset checks
- **Completeness:**
  - Distinct event_ids per type in S3 equal `manifest.json`, for CheckIn, CheckOut and
    PlantMovement only.
  - Evaluations equal distinct check-ins.
  - Runs only after the generator's flush wait.
- **DLQ:** the DLQ holds only messages with `poison = true`, read through boto3.
- **Freshness:** live mode only. The latest received_time is within 15 minutes of now.
  In backfill mode the check is skipped.

### 6.5 Firehose specifics
- The EventBridge target uses an input transformer, template `<aws.events.event>` plus
  a newline. That gives one event per line.
- The buffer is 60 s / 1 MB. The default S3 prefix is the arrival hour; partitioning by
  event date is left out.
- Delivery is at least once, so staging deduplicates.
- The generator ends by polling S3 until the counts match the manifest, or a timeout
  passes.

## 7. Testing strategy

1. **Unit (pytest, TDD):**
   - `rules.py` over `golden_cases.csv`:
     - daylight saving;
     - an intervening visit;
     - most-specific scope;
     - the pick order;
     - late insert;
     - placeholder exclusion;
     - window edge;
     - clipping at 0.
   - `handler.py` with moto:
     - dedupe and redelivery re-emit;
     - an orphan CheckOut followed by its CheckIn;
     - a version conflict and retry;
     - partial batch failure;
     - trimming.
2. **dbt:** the tests in §6.3. The CI target reads `--local-out` files.
3. **Integration:** the full chain on LocalStack. Send a small fixed event set,
   including out-of-order and duplicate cases, then assert the person_state items, the
   S3 objects, and the evaluations and alerts.
4. **Parity:** over the full generated dataset, `false_alert`, `not_evaluated` and
   `unexplained` = 0.

## 8. CI/CD and deployment

- **GitHub Actions:**
  - ruff, pytest, `terraform fmt -check` and `validate`, and `dbt build` on a small
    `--local-out` dataset.
  - A separate LocalStack integration job, with a token from repository secrets if
    needed.
- **Makefile:** `up`, `deploy ENV=local|aws`, `setup`,
  `generate MODE=backfill|live N=…`, `poison K=…`, `dagster`,
  `trace PROPERTY=… START=…`, `destroy ENV=…`, `test`.
- **Real AWS:**
  - Region ap-southeast-2, local Terraform state.
  - A $5 budget alarm is created first; `make destroy` runs after the screenshots,
    which are stored in `docs/`.
  - Lambda is packaged with `archive_file` (stdlib plus boto3 from the runtime).
- **LocalStack:** since release 2026.03.0, the single image needs
  `LOCALSTACK_AUTH_TOKEN`. Day 1 decides between the free hobby token and pinning a
  pre-2026.03 4.x tag.
  - Fallback if neither works: run the handler in-process against moto through a local
    driver. That is a **reduced** demo; the full chain is then shown only on real AWS.

## 9. Demo script (about 5 minutes)

Before the demo, `make generate MODE=backfill` and one Dagster run are already done.

1. The README architecture and how it maps to Onside: check-ins, stand-down, Tracing,
   "Key property identification" (30 s).
2. `make generate MODE=live N=50`: events flow, and HighRiskVisit alerts appear in the
   Lambda logs (1 min).
3. Dagster UI: lineage, a run, green checks (1 min).
4. Reconciliation: "X breaches were missed in real time because the origin was
   offline. Here is the evidence from the evaluation log. That's why the warehouse
   recomputes on event_time." (1.5 min)
5. `make trace`: the earliest arrival per downstream property, then the PageRank key
   properties (1 min).
6. Production next steps: incremental models with a lookback, Iceberg + Athena,
   ClickHouse for customer-facing reports, rules per property and group plus outbreak
   overrides, data residency (30 s).

## 10. Schedule

| Day | Work |
|---|---|
| D1 | Repo, LocalStack decision, generator (including `--local-out`), `rules.py` with golden cases |
| D2 | Terraform and the Lambda handler with moto tests; LocalStack integration if time |
| D3 | dbt models and tests on `--local-out` data, including reconciliation; LocalStack integration if not done |
| D4 | dbt on S3, Dagster (assets, checks, trace job), CI |
| D5 | Real AWS deploy, README, demo rehearsal |

**Cut line**, in this order, if time runs short:
1. plant-material edges in tracing;
2. the S3 sensor (keep the schedule only);
3. the network Python model;
4. the Firehose path. In that case the generator writes raw files to S3 directly, and
   evaluations still go through EventBridge.

## 11. Decisions

- **Standard SQS rather than FIFO.** EventBridge does not guarantee order, so FIFO
  would not give per-person order anyway. It also complicates partial batch failure
  (the batch must stop at the first failure). Order independence plus optimistic
  locking is the real guarantee.
- **Evaluations are logged** (`CheckInEvaluated`), so every missed alert has evidence
  and an explained cause, not one guessed from timestamps.
- **The stand-down end ignores inferred check-outs.** This keeps the realtime side a
  subset of batch, so the "realtime can only miss" guarantee (§4) holds.

## 12. Review log

**Review 1 (2026-10-09): 9 blocking issues, all fixed in this revision.**

| # | Issue | Fix |
|---|---|---|
| B1 | No time model for the replay | §3.1 |
| B2 | Window anchor ambiguous | §4.3, §6.1 trimming |
| B3 | Dedupe written before the work was committed | §6.1 steps 3–6 |
| B4 | Check-out matching undefined | §4.1, §6.1 step 4, §3.2 |
| B5 | No source of reference data for the Lambda | §3.4 |
| B6 | Rule selection and grain ambiguous | §4.5–4.8 |
| B7 | False FIFO ordering assumption | §6.1, §11 |
| B8 | Miss causes unprovable, no catch-all | §6.2, CheckInEvaluated |
| B9 | Tracing undefined and exploding | §5 Tracing |

Non-blocking N2–N11 and N13–N14 were also adopted. N1 and N12 no longer apply after
the switch to standard SQS.
