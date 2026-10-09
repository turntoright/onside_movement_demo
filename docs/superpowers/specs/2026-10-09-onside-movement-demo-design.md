# Onside movement demo — design

Date: 2026-10-09. Revision 6.1: review 6 found no blocking issues; its non-blocking
notes are folded in (see §12).

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
- Trimming per-person state (§6.1 explains why the demo doesn't need it).
- The real product's rules between individual properties and groups, and temporary
  outbreak overrides. The demo models rules between **property types** only.

## 2. Architecture

```
 Generator (Python)
   ├─ setup: reference data → DynamoDB `properties` + S3 ref/ (same snapshot)
   ├─ PutEvents in received_time order: CheckIn / CheckOut / PlantMovement
   └─ settle & compact: S3 compacted/<env>/<run_id>/*.ndjson + control/settled/<run_id>.json
        ▼
 EventBridge bus: onside-events
   ├─ rule A (source=onside.app, CheckIn|CheckOut)
   │     ──► SQS visit-queue (standard, DLQ after 5 receives)
   │           ──► Lambda standdown_checker
   │                 ├─ DynamoDB properties     (read, cached per container)
   │                 ├─ DynamoDB person_state   (one item per person, versioned)
   │                 ├─ DynamoDB evaluations    (one item per check-in)
   │                 └─ after commit: PutEvents (source=onside.standdown)
   │                       CheckInEvaluated, CheckOutApplied, HighRiskVisitDetected
   └─ rule B (all sources, excluding detail.poison) ──► Firehose (no transform)
                              ──► S3 raw/ (arrival-hour prefixes; landing only)
                                    │
 Dagster (S3 sensor on control/settled/ + schedule)
   └─ dbt-duckdb reads S3 compacted/ → models → tests → DuckDB file
        └─ outputs: movements, network, high-risk visits, reconciliation,
                    key properties; trace job (read-only)
```

- **Rule A:** `source = onside.app` and detail-type CheckIn or CheckOut, with **no**
  poison filter. Poison events must reach the queue to demo the DLQ. The source filter
  also keeps the Lambda's own events from looping back.
- **Rule B:** `source` in [onside.app, onside.standdown], with
  `"detail": {"poison": [{"exists": false}]}`, so poison events never reach S3. Day 1
  verifies `exists:false` on LocalStack.

### Components

| Unit | Responsibility | Depends on |
|---|---|---|
| `generator/` | Builds reference data and the event stream; loads reference data; emits events; writes manifests; settles and compacts (§3.4) | boto3 |
| `lambdas/standdown_checker/rules.py` | Pure stand-down logic (§4) | stdlib, tzdata |
| `lambdas/standdown_checker/processor.py` | Event processing over a `StateStore` interface (§6.1). Shared by the Lambda and the local simulator | rules |
| `lambdas/standdown_checker/dynamo_store.py` | `StateStore` on DynamoDB: reads, plus TransactWriteItems commits | boto3 |
| `lambdas/standdown_checker/handler.py` | SQS batch wrapper: partial batch failures, PutEvents with `FailedEntryCount` checked | processor |
| `generator/local_sim.py` | `StateStore` in memory, persisted to `state/sim/sim_store.json` between runs. Runs `processor` in received_time order for `ENV=sim` | processor |
| `shared/standdown_rules.csv` | The single source of rules: bundled into the Lambda zip and used as a dbt seed | — |
| `shared/golden_cases.csv` | Pure-rule edge cases, run by both pytest and dbt (§6.3) | — |
| `infra/terraform/` | All AWS resources; a variable switches between LocalStack endpoints and real AWS | — |
| `dbt/` | Staging → intermediate → marts, tests, a Python model for network metrics | DuckDB, S3 |
| `orchestration/` | Dagster definitions: dbt assets, sensor, asset checks, trace job | dbt, boto3 |

- Validation uses stdlib dataclasses, so the Lambda zip has no compiled dependencies.
- The zip bundles the pure-Python `tzdata` package, so `zoneinfo` never depends on
  the runtime's time-zone files.

## 3. Data and time

### 3.1 Time model
- Every timestamp is ISO 8601 with a UTC offset. "Local" always means
  `Pacific/Auckland`.
- `event_time` is when it happened on the device. `received_time` is when the backend
  received it. The generator fixes both.
- The generator emits events in `received_time` order, in PutEvents batches of 10.
- The Lambda never uses its own clock for business logic:
  - `detected_time` = the `received_time` of the check-in being evaluated.
  - Wall-clock `processed_at` is kept for operations only.
  - Ordering proofs use DynamoDB item **versions**, not clocks (§6.2).

### 3.2 Runs: backfill, then live
- **`generate --backfill`** (run once before the demo):
  - 12 months of history ending at the wall-clock time when it starts.
  - It saves the generator state (each person's last visit, the counters and the seed)
    to `state/<env>/generator_state.json`.
  - It takes tens of minutes on LocalStack.
- **`generate --live N`**:
  - Loads the generator state and **continues each person's timeline**.
  - Emits N check-ins (plus their check-outs) with `event_time` in the last 30 minutes.
    Some events are received up to 10 minutes late.
  - Each person's live check-in has `event_time` after that person's last visit end.
    People with no room in the 30-minute window are skipped, so "no overlap" holds
    even straight after a backfill.
  - A visit still open at the end of a run never gets a check-out. Downstream infers
    one (§5). Later runs never send it, so the live lateness bound holds.
  - About 20% of the live check-ins are deliberate stand-down breaches, so alerts
    appear in the demo.
  - It updates the generator state.
- **Event ids:** `event_id = uuid5(run_id, sequence)`, so runs never collide.
- **Environments:** every generator command takes `ENV=local|aws|sim`.
  - **`local`** (LocalStack) and **`aws`:**
    - Generator state and manifests live under `state/<env>/` and are mirrored to that
      environment's S3 `control/manifests/`.
    - Settling and the asset checks read only the current environment's manifests, so
      LocalStack and AWS never mix.
  - **`sim`:** makes no AWS calls (§3.3 Local output). Used by CI and for dbt
    development.
  - `make up` resets `state/local/` and LocalStack's Terraform state, because a
    LocalStack restart wipes its DynamoDB and S3.
  - `make up` does a full restart (compose down, then up, with persistence off), so the
    stack is always empty after it.
  - `make reset ENV=sim` clears `state/sim/` and `data/sim/` together.
  - `make destroy ENV=aws` also clears `state/aws/`.
  - A backfill refuses to run when the environment already has generator state. A
    failed or partial backfill is recovered only by a reset.
  - Every `generate` first checks that S3 `control/manifests/` matches
    `state/<env>/`. If LocalStack lost its data (e.g. after a Docker restart), it fails
    with a clear message telling you to run `make up`.
  - `state/` and `data/` are gitignored.
- **Manifests:**
  - Each backfill or live run writes `control/manifests/<run_id>.json` (mode, seed,
    distinct event_id counts per type) **before** emitting.
  - Poison runs never write here; they write only `control/poison/` (§3.3).
  - The checks sum the environment's manifests.
- **Scale per environment:**
  - LocalStack: the default backfill.
  - AWS: a small backfill that scales people and properties down with it (about 100
    people, 40 properties, 3,000 check-ins) plus one live run. It still shows movements
    and breaches, and keeps DynamoDB transactional write costs well under the budget.
  - sim in CI: a small dataset (about 200 people, 5,000 check-ins).
- **Clamping:** received_time is clamped to the run's end, so nothing is "received"
  in the future.

### 3.3 Generator guarantees (every run)
- **Seed:** fixed, so output is reproducible relative to the anchor time (the end of
  the backfill).
- **Default scale:** about 500 properties, 2,000 people, 100,000 check-ins.
  - At most 300 check-ins per person across all runs.
- **Properties:** type (poultry, pork, beef, dairy, kiwifruit, vineyard, office, feed
  mill), group, `owner_company_id`, region, coordinates.
  - The snapshot is static (no history of changes).
- **Movement distance:** about 70% of moves between properties are under 10 km, with
  rare long moves (from the paper's 71.3%).
- **Offline:** about 5% of check-ins and check-outs arrive late.
  `received_time − event_time` is at most 7 days in backfill and 10 minutes in live.
- **Duplicates:** about 2% of events are sent again, same event_id and body.
- **Check-outs:**
  - Every CheckOut belongs to an existing CheckIn.
  - There is **at most one** distinct CheckOut per check-in.
  - check-in ≤ check-out ≤ check-in + **48 hours**.
- **No overlap:** a person's visits don't overlap.
- **`additional_visitors`:** an integer count. It is reported, never expanded into
  movements or stand-down checks.
- **Poison:** only valid events go on the normal path. `generate --poison K` sends K
  malformed events with `detail.poison = true`, recorded in
  `control/poison/<run_id>.json`.
  - Their detail-type is CheckIn or CheckOut, so rule A routes them.
  - A poison run does not settle; it never waits for S3.
- **Local output (`ENV=sim`):** a complete third environment.
  - No AWS calls.
  - Generator state, manifests and the persisted sim store live in `state/sim/`.
    Each run loads the store and saves it at the end, so versions continue across
    backfill and live runs, as they do in DynamoDB.
  - Each run writes **full EventBridge envelopes** (source, detail-type, time, detail)
    for its generator events, plus the evaluation, applied and alert envelopes from
    `local_sim`. They go to `data/sim/compacted/<run_id>/*.ndjson`, the same layout as
    S3 `compacted/<env>/<run_id>/`.
  - `setup ENV=sim` writes the reference snapshot to
    `data/sim/ref/{properties,people,companies}.json`. `local_sim` and live runs load
    it from there.
  - Each dbt target sets two variables, and the SQL is otherwise the same:

    | Target | `compacted_glob` | `ref_root` |
    |---|---|---|
    | `sim` | `data/sim/compacted/*/*.ndjson` | `data/sim/ref/` |
    | `local`, `aws` | `s3://<bucket>/compacted/<env>/*/*.ndjson` | `s3://<bucket>/ref/` |

### 3.4 Settle and compact
LocalStack's Firehose may write one small S3 object per event, and the backfill could
produce hundreds of thousands of them. So nothing downstream reads `raw/` directly.

`generator/settle.py` runs after emitting:
1. **Scan incrementally.**
   - List only arrival-hour prefixes from one hour before the run's start onwards.
     The extra hour absorbs clock skew.
   - Skip keys already parsed. A local key cache lives in `state/<env>/`.
2. **Parse, spool and count.**
   - Parse each new object (concatenated JSON envelopes).
   - Append its envelopes to a spool file `state/<env>/spool/<run_id>.ndjson`, so
     nothing is held in memory.
   - Count only **this run's** event_ids per type: `event_id = uuid5(run_id, seq)` with
     `seq` below the manifest's total, so `make settle` rebuilds the set from the
     manifest alone. Each event's type comes from its envelope.
   - Leftovers from earlier runs can then never satisfy the gate early.
   - Append to the spool **before** updating the key cache, so a crash causes
     duplicates, never loss.
3. **Settled** when all of these hold:
   - every manifest event is present;
   - there is one CheckInEvaluated per distinct check-in and one CheckOutApplied per
     distinct check-out;
   - there is one HighRiskVisitDetected per evaluation with `breach = true`;
   - the SQS queue shows 0 visible and 0 in-flight messages;
   - one more Firehose buffer (60 s) has passed, after which the scan is repeated.
4. **Compact.**
   - Write the spool as a few NDJSON files under `compacted/<env>/<run_id>/`.
   - Write `control/settled/<run_id>.json`, then exit.
   - On a timeout, exit non-zero and keep the spool. `make settle RUN=… ENV=…` resumes
     from the spool and the key cache.
   - Duplicates across runs are harmless, because staging deduplicates over all of
     `compacted/<env>/*/`.

dbt reads only `compacted/`, which is a few files per run.

**Scale checks:**
- **Day 1:** measure the Firehose object count and the parse throughput at 10,000
  events, using a stub queue consumer.
- **Day 4:** measure Lambda throughput on LocalStack.
- If a full backfill would take more than about 45 minutes, the LocalStack default
  scale drops to 30,000 check-ins.

### 3.5 Events (`detail` of each EventBridge event)

| Event | Source | Fields |
|---|---|---|
| `CheckIn` | onside.app | event_id, person_id, visitor_company_id, property_id, event_time, received_time, method (app, geofence, qr, kiosk, link), job_type, additional_visitors, answers[] |
| `CheckOut` | onside.app | event_id, checkin_event_id, person_id, property_id, event_time, received_time, method (manual, geofence_auto, end_of_day) |
| `PlantMovement` | onside.app | event_id, source_property_id, dest_property_id, movement_date (local date), material_type, volume, received_time |
| `CheckInEvaluated` | onside.standdown | event_id = hash("eval", checkin_event_id), checkin_event_id, person_id, version_read, version_committed, detected_time, processed_at, origins_seen[] (origin checkin_event_id, end, end_source, nights_between), breach, rule_id, origin_checkin_event_id, required_nights, nights_between |
| `CheckOutApplied` | onside.standdown | event_id = hash("co", checkout_event_id), checkout_event_id, checkin_event_id, person_id, version_committed, processed_at |
| `HighRiskVisitDetected` | onside.standdown | event_id = hash("alert", checkin_event_id), checkin_event_id, person_id, origin_property_id, dest_property_id, origin_checkin_event_id, nights_between, required_nights, rule_id, detected_time, processed_at |

PlantMovement does not go to the Lambda. It is used only by tracing (§5).

### 3.6 Reference data
- The `properties` snapshot (property_id, type, group, owner_company_id, region, lat,
  lon) is written once, at setup, to DynamoDB `properties` and S3
  `ref/properties.json`.
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
   - Inferred check-outs (§5) are never used here.
2. **Nights:**
   `nights_between(O, D) = max(0, local_date(D.checkin_time) − local_date(end(O)))`,
   as an integer of local dates.
3. **Window:** O counts only if `nights_between(O, D) ≤ 14`. Every rule has nights ≤ 14
   and a breach needs `nights_between < nights`, so the window never changes the
   result.
4. **Scope:** **internal** if `O.property.owner_company_id = D.property.owner_company_id`,
   otherwise **external**. The visitor's company plays no part.
5. **Rule for one (O, D) pair:** among the rules with from_type = O.type and
   to_type = D.type, the most specific scope wins (the matching internal or external
   rule beats `any`). If no rule matches, the pair has no constraint.
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

The Lambda sees a subset of batch's origin candidates, and for each one an end that is
≤ batch's end. A check-out not yet seen gives `checkin_time`, and there is at most one
distinct check-out per check-in. Scope and rule choice depend only on the static
snapshot. The (time, id) order is the same on both sides.

**`origins_seen` bound.** Visits last at most 48 hours (§3.3). So any origin batch could
find breaching has `local_date(D) − local_date(O.checkin) ≤ 16`. The evaluation
records every observed origin candidate inside that bound, **before** the 14-night
filter, each with the end and nights the Lambda used.

## 5. dbt models

### Staging
- Events: `stg_checkins`, `stg_checkouts`, `stg_plant_movements`, `stg_evaluations`,
  `stg_checkout_applied`, `stg_realtime_alerts`.
- Reference data, read from `var('ref_root')`: `stg_properties`, `stg_people`,
  `stg_companies`.
- **Reading:**
  - Read the target's glob (§3.3 Local output). Use `read_json(glob, format='newline_delimited', columns={...})`, declaring
    `detail` as JSON (no schema inference).
  - Then extract the fields per detail-type.
  - Only `settle.py` reads the concatenated `raw/` objects.
- **Filtering and dedupe:**
  - Drop any row with `detail.poison`.
  - Deduplicate events on event_id. Evaluations, applied events and alerts go on their
    natural key: checkin_event_id, checkout_event_id and checkin_event_id. Their bodies
    are identical; the tie-break is earliest `processed_at`.

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
  - The SQL lives in a macro, `standdown(visits_relation, properties_relation,
    rules_relation)`.
  - The golden-case test (§6.3) calls the same macro.
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
- **`dim_property_network`** (dbt Python model, networkx): built from all people
  movements, with edge weight = movement count. Columns: in-degree, out-degree,
  weighted strength, weighted PageRank. This is "key property identification".

### Tracing
A Dagster job with config property_id, start_time and max_hops (default 3). It opens
DuckDB read-only. `make trace` submits it through the Dagster run queue, so the
concurrency pool applies. The algorithm is a pure-Python module with its own pytest
tests.
- **Edges:**
  - **People:** `connected` movements. Departure = the origin `visit_end`
    (inferred allowed). Arrival = the destination check-in.
  - **Plant material:** departure = start of `movement_date` (local), arrival = end of
    `movement_date`.
- **A hop is valid** when `departure ≥ arr(origin)`.
- **Algorithm:** a hop-limited Bellman-Ford over layers r = 0..max_hops.
  - Start: `arr_0(seed) = start_time`, and every other property is unreachable.
  - Each round: `arr_r(v) = min(arr_{r−1}(v), min over valid edges u→v of arrival)`,
    where validity is checked against `arr_{r−1}(u)`.
  - Only properties that improved in round r−1 are relaxed in round r.
  - Stop early when nothing improves.
- **Witness:** stored per layer in `pred(r, v)`.
  - If `arr_r(v) = arr_{r−1}(v)`, it inherits `pred(r−1, v)`.
  - Otherwise it is `(u, r−1)`.
  - Following it backwards gives a path of at most r hops.
- **Output:** for each reachable property: `arr_max(v)`, its witness path, and
  hops = the path's length.

All models are rebuilt in full. The README explains the production alternative:
incremental by received_time, a lookback window, and recomputing the affected people.

## 6. Error handling and data quality

### 6.1 Lambda `standdown_checker`

**Queue.**
- SQS standard queue with `ReportBatchItemFailures`.
- Visibility timeout 30 s; Lambda timeout 25 s (it must not exceed the visibility
  timeout).
- DLQ after 5 receives, with 14-day retention.
- A poison message reaches the DLQ after about 2.5 minutes. `make poison` waits until
  the DLQ count is at least the expected value, with a 5-minute timeout. A duplicate
  delivery may push it above; the DLQ check then uses ≥.
- Delivery order is **not** assumed, so the processor is order-independent.

**State.**
- **`person_state`**, one item per person:
  - `version`;
  - `visits[]`, each with checkin_event_id, property_id, type, owner_company_id,
    checkin_time (null for a placeholder), checkout_event_id, checkout_time and
    checkout_applied_version.
  - Each check-out also stores `checkout_processed_at`, so re-emits are
    byte-identical.
  - **No trimming.** At most 300 visits × about 350 bytes (attribute names included)
    is about 100 KB, against the 400 KB item limit. Production would trim and keep
    dedupe in a separate store; the README names this.
  - **Missing item:** a person with no item yet reads as `version_read = 0` and
    `visits = []`. The first commit creates version 1.
- **`evaluations`**, one item per checkin_event_id: the full CheckInEvaluated body.

**Commit.** Each change is one `TransactWriteItems` call:
- an update of `person_state` with the condition
  `attribute_not_exists(person_id) OR version = :read`, which sets
  `version = :read + 1`;
- for a check-in, a put of the evaluation with the condition
  `attribute_not_exists(checkin_event_id)`.

**Reads and retries.**
- Reads use `ConsistentRead=True`.
- On `TransactionCanceled`:
  - If the evaluation put failed its condition, take the "already processed" path.
  - Otherwise, retry from step 2 (including the duplicate check), with jittered
    backoff, up to 3 times. After that, fail the record.
- The in-memory store implements the same conditions.

**Processing one record:**
1. **Validate.** If invalid, the record fails (only poison events should).
2. **Load** `person_state` and remember `version_read`.
3. **CheckIn X:**
   - **Already processed** (an evaluation for X exists): re-emit the stored evaluation
     and, for a breach, its alert. The alert's property ids are rebuilt from
     `visits[]`. Done.
   - **Otherwise:**
     - Look up the property (cached).
     - Evaluate per §4 against the visits with an observed check-in.
     - Insert X at its sorted position, or fill the placeholder for X if one exists
       (keeping its check-out).
     - Commit the visit and the evaluation together.
   - The duplicate test is "an evaluation exists", never "the id is in visits[]", so a
     placeholder is never mistaken for a processed check-in.
4. **CheckOut C for check-in X:**
   - If C is already applied to X: re-emit CheckOutApplied with the stored version and
     `checkout_processed_at`. Done.
   - If X exists in visits[] (placeholder or not): record C, keeping the earliest if
     the visit already has a check-out.
   - Otherwise: create a placeholder for X holding C.
   - Commit, storing `checkout_applied_version = version_committed`.
5. **After the commit:** PutEvents. A non-zero `FailedEntryCount` counts as a failure.
   - Bodies come from stored data, so re-emitting is byte-identical.
   - On failure the record fails. The redelivery takes the "already processed" path
     and re-emits.

**Why redelivery is safe.** Nothing is trimmed, so a redelivered or duplicated event
always finds its own committed result. The PutEvents and visibility-timeout delays don't
matter.

### 6.2 Reconciliation (`fct_alert_reconciliation`)
- **Realtime breach** means `stg_evaluations.breach = true`. A test checks that an
  alert exists if and only if `breach = true`.
- **Rows:** one per checkin_event_id that is a breach on either side.
- **Missed rows:** O* is batch's reported origin. Versions belong to the same person
  item, so "committed after D was read" means `version_committed > D.version_read`.

| Class | Rule | Expectation |
|---|---|---|
| `matched` | Both breach. Info columns: `same_attribution` (rule_id and origin), and `late_alert = (D.received_time − D.event_time > 1 h)`. | Most rows |
| `missed` / `origin_not_seen` | O* is not in `origins_seen`, **and** O*'s evaluation has `version_committed > D.version_read`. Sub-cause: `offline` if O*.received_time > D.received_time, otherwise `reordered`. | Some |
| `missed` / `origin_end_not_seen` | O* is in `origins_seen` with `end_source = checkin`, **and** O*'s CheckOutApplied has `version_committed > D.version_read`. | Few |
| `missed` / `not_evaluated` | No evaluation for D. | **Must be 0** |
| `missed` / `unexplained` | Anything else, including a version proof that fails. | **Must be 0** |
| `false_alert` | Realtime breach, batch no breach. | **Must be 0** |

### 6.3 Tests
- **Generic dbt tests:**
  - unique and not-null on event_id in staging;
  - relationships for property_id and person_id;
  - accepted values for method and type;
  - (from_type, to_type, scope) unique in the rules seed.
- **Singular dbt tests:**
  - at most one distinct check-out per checkin_event_id;
  - check-in ≤ check-out ≤ check-in + 48 h;
  - movement origin ≠ destination;
  - every rule has 1 ≤ nights ≤ 14;
  - every evaluation's `origins_seen` equals the batch origin candidates within the
    16-day bound whose own `CheckInEvaluated.version_committed` ≤ D.version_read.
  - each entry has `end_source = checkout` if and only if O's
    `CheckOutApplied.version_committed` ≤ D.version_read.
  - alert iff `breach = true`;
  - completeness: one evaluation per check-in and one applied event per check-out.
- **Golden cases** (`shared/golden_cases.csv`, pure rules only):
  - Columns: case_id, role (origin or target), visit_id, property_id, property_type,
    owner_company_id, checkin_time, checkout_time, expected_breach, expected_rule_id,
    expected_origin_visit_id.
  - A staging model splits the seed into the visits and properties relations that the
    `standdown` macro expects.
    - It sets `person_id = case_id` and `property_id = case_id || ':' || property_id`.
    - So the macro needs no knowledge of cases.
  - Cases include a same-property origin, which must be excluded.
  - Cases: daylight saving, an intervening visit, most-specific scope, the pick order,
    the window edge, clipping at 0, no matching rule.
  - Pytest runs `rules.py` over it. A dbt singular test runs the `standdown` macro
    over the seed and compares.
- **dbt unit tests** (SQL models): inferred check-outs in `int_visits`, and every
  class in `fct_alert_reconciliation`.
- **Reconciliation:** `false_alert`, `not_evaluated` and `unexplained` = 0 at error
  level; the missed rate above a threshold at warn level.

### 6.4 Dagster
- **Environment:**
  - Dagster runs only for `local` and `aws`. `sim` uses plain `dbt build`.
  - `make dagster ENV=…` sets the dbt target, the bucket and the DuckDB file
    `warehouse/<env>.duckdb` (one per environment; sim uses `warehouse/sim.duckdb`).
  - All local paths in dbt vars are absolute, built from a `REPO_ROOT` environment
    variable that the Makefile sets.
  - `make trace ENV=local|aws` goes through Dagster. `make trace ENV=sim` calls the
    trace module directly on `warehouse/sim.duckdb`.
- **Sensor:** fires on new `control/settled/` objects, never on raw objects. A
  schedule exists as a backup.
- **Concurrency:** dbt runs and the trace job share a concurrency pool of size 1, to
  avoid DuckDB file-lock conflicts.
- **Asset checks:**
  - **Completeness:** distinct event_ids per type in `compacted/<env>/` equal the
    environment's summed manifests, for CheckIn, CheckOut and PlantMovement.
  - **DLQ:** `ApproximateNumberOfMessages` ≥ the summed `control/poison/` manifests,
    and the DLQ holds no more than 1 extra per poison event. Read the attribute only;
    never receive messages.
  - **Freshness:** after a live run, the latest received_time is within 15 minutes of
    the settle time. Skipped for backfill.

### 6.5 Firehose
- Rule B targets Firehose with **no input transformer**, so the full EventBridge
  envelope, including `detail`, is delivered.
- Records are concatenated without a delimiter. Only `settle.py` reads them, with a
  streaming JSON decoder (`json.JSONDecoder.raw_decode`).
- The buffer is 60 s / 1 MB on AWS. LocalStack may ignore it.
- The default prefix is the arrival hour.
- Delivery is at least once, so both settle and staging deduplicate.
- Day 1 verifies delivery, envelope content and the object count on LocalStack.

## 7. Testing strategy

1. **Unit (pytest, TDD):**
   - `rules.py` over the golden cases.
   - `processor.py` over the in-memory store:
     - late insert;
     - a placeholder followed by its CheckIn (evaluated, not skipped);
     - duplicate and redelivery re-emitting byte-identical bodies;
     - earliest check-out kept;
     - the `origins_seen` bound.
   - `dynamo_store.py` and `handler.py` with moto:
     - a transaction conflict and retry;
     - partial batch failure;
     - `FailedEntryCount` treated as a failure.
2. **dbt:** the tests in §6.3. CI generates `ENV=sim` data (a backfill and one live
   run), including the simulator's evaluations, so reconciliation runs in CI too.
3. **Integration:** the full chain on a **fresh** LocalStack stack. The order is
   `make up` → `make integration` → `make up` (reset) → deploy → setup → backfill.
   Its orphan, duplicate and poison cases then never mix with demo data. Send a small fixed set including
   out-of-order, duplicate, orphan check-out and poison cases. Wait for settling. Then
   assert person_state, evaluations, the S3 objects and the DLQ count.
4. **Parity:** over the full generated dataset, on both `sim` and LocalStack:
   `false_alert`, `not_evaluated` and `unexplained` = 0.

## 8. CI/CD and deployment

- **GitHub Actions:**
  - ruff, pytest, `terraform fmt -check` and `validate`, and `dbt build` on a small
    `ENV=sim` dataset.
  - A separate LocalStack integration job, with a token from repository secrets if
    needed.
- **Makefile:**
  - Infrastructure: `up`, `deploy ENV=local|aws`, `destroy ENV=local|aws`.
  - Data: `setup ENV=…`, `generate ENV=… MODE=backfill|live N=…`,
    `settle ENV=… RUN=…`, `poison ENV=local|aws K=…`, `reset ENV=sim`.
  - Run: `dagster ENV=local|aws`, `trace ENV=… PROPERTY=… START=…`.
  - Tests: `test` (unit plus dbt on sim), `integration` (fresh LocalStack).
- **Terraform state:**
  - Kept separately per environment: `infra/terraform/state/<env>.tfstate`, passed
    via `-state`. A backend config per environment works too.
  - The AWS bucket name includes the account id, so it is globally unique. The bucket
    has `force_destroy = true`, so `make destroy` works on a non-empty bucket.
- **Real AWS:**
  - Region ap-southeast-2, local Terraform state.
  - A $5 budget alarm is created first; `make destroy` runs after the screenshots,
    which are stored in `docs/`.
  - Lambda packaged with `archive_file` (stdlib, tzdata, boto3 from the runtime).
- **LocalStack:** since release 2026.03.0, the single image needs
  `LOCALSTACK_AUTH_TOKEN`. Day 1 decides between the free hobby token and pinning a
  pre-2026.03 4.x tag.
  - Fallback if neither works: `ENV=sim` covers everything except
    the AWS wiring. That is a reduced demo; the full chain is then shown only on real
    AWS.

## 9. Demo script (about 5 minutes)

Before the demo, `make generate MODE=backfill` has settled and Dagster has run once.

1. The README architecture and how it maps to Onside: check-ins, stand-down, Tracing,
   "Key property identification" (30 s).
2. `make generate MODE=live N=50`: events flow, and HighRiskVisit alerts appear in the
   Lambda logs. The run settles in about 2 minutes (1 min shown).
3. Dagster UI: the sensor-triggered run, lineage, green checks (1 min).
4. Reconciliation: "X breaches were missed in real time. Y because the origin was
   offline, Z because of delivery order. Here is the version evidence. That's why the
   warehouse recomputes on event_time." (1.5 min)
5. `make trace`: the earliest arrival per downstream property, then the PageRank key
   properties (1 min).
6. Production next steps: incremental models with a lookback, state trimming with a
   separate dedupe store, Iceberg + Athena, ClickHouse for customer-facing reports,
   rules per property and group plus outbreak overrides, data residency (30 s).

## 10. Schedule

| Day | Work |
|---|---|
| D1 | Repo; LocalStack decision; Firehose → S3 check and scale measurement at 10k events; generator; `rules.py` with golden cases |
| D2 | `processor.py` + `local_sim` (TDD), so `ENV=sim` produces evaluations; start Terraform |
| D3 | dbt models and tests on `sim` data, including reconciliation; trace algorithm (pure Python) |
| D4 | Terraform finished, DynamoDB store, handler, settle & compact, LocalStack integration; **full backfill in the evening** |
| D5 | Dagster (sensor, checks, trace job), CI, real AWS deploy, README, rehearsal |

D5 is tight. The cut line protects it.

**Cut line**, in this order, if time runs short:
1. plant-material edges in tracing;
2. the LocalStack CI job (integration stays a local `make` target);
3. the network Python model;
4. the S3 sensor (keep the schedule only);
5. the trace Dagster job (keep `make trace` calling the module directly);
6. the Firehose path. In that case the Lambda and the generator write their events
   to S3 directly.

## 11. Decisions

- **Standard SQS rather than FIFO.** EventBridge does not guarantee order, so FIFO
  would not give per-person order anyway, and it complicates partial batch failure.
  Order independence plus version-checked commits is the real guarantee.
- **Evaluations are logged with versions**, so every missed alert has an explained,
  provable cause, not one guessed from clocks or timestamps.
- **The stand-down end ignores inferred check-outs.** This keeps the realtime side a
  subset of batch, so "realtime can only miss" holds (§4).
- **No state trimming.** Bounded by the generator's 300 check-ins per person. This
  removes a whole class of redelivery bugs from the demo; production trimming is a
  named next step.
- **One processor, two stores.** The Lambda and the local simulator share
  `processor.py`, so CI can run reconciliation without AWS.
- **Raw is landing only.** dbt reads compacted NDJSON, so it never depends on how many
  objects Firehose writes.
- **State is per environment.** LocalStack and AWS each have their own generator
  state, manifests and scale.

## 12. Review log

**Review 1: 9 blocking issues, fixed in revision 2.**

| # | Issue |
|---|---|
| B1 | No time model for the replay |
| B2 | Window anchor ambiguous |
| B3 | Dedupe written before the work was committed |
| B4 | Check-out matching undefined |
| B5 | No source of reference data for the Lambda |
| B6 | Rule selection and grain ambiguous |
| B7 | False FIFO ordering assumption |
| B8 | Miss causes unprovable |
| B9 | Tracing undefined and exploding |

**Review 2: 9 blocking issues, fixed in revision 3.**

| # | Issue | Fix |
|---|---|---|
| B-1 | A placeholder was taken for a duplicate CheckIn | §6.1 step 3: duplicate means "an evaluation exists" |
| B-2 | The Firehose input transformer dropped `detail` | §6.5: no transformer (superseded in revision 4: only `settle.py` reads raw; dbt reads NDJSON) |
| B-3 | Trimming broke redelivery and dedupe | §6.1: no trimming; separate `evaluations` table; transactional commit |
| B-4 | Loose miss classes; `origins_seen` undefined | §4 bound; §6.2 version proofs; CheckOutApplied |
| B-5 | Evaluations arrive after check-ins | §3.4 settling; sensor on `control/settled/` |
| B-6 | `--local-out` had no evaluations | `local_sim` sharing `processor.py` (`ENV=sim` since revision 5) |
| B-7 | Poison events reached S3 and staging | Event pattern exclusion plus a staging filter |
| B-8 | Live mode's relation to backfill undefined | §3.2: continuation, uuid5 ids, per-run manifests |
| B-9 | Witness path and hop count could disagree | §5: per-layer predecessors |

Non-blocking items from review 2 were also adopted:
- N1: `owner_company_id`.
- N2: alert iff breach.
- N3: the window test replaced.
- N4: golden CSV schema and a shared macro.
- N5: tzdata bundled.
- N6: `FailedEntryCount` checked.
- N7: DLQ after 5 receives; the attribute is read, never received.
- N8: explicit columns.
- N9: concurrency pool, read-only trace.
- N10: cut item 4.
- N11: offline and reordered split.

**Review 3: 4 blocking issues, fixed in revision 4.** It confirmed that the review-2
fixes, the version proofs, the 16-day bound (tight across daylight saving) and the
tracing algorithm are sound.

| # | Issue | Fix |
|---|---|---|
| 1 | Rule A's poison filter contradicted the DLQ demo | §2: rule A has no poison filter |
| 2 | Commit condition failed on a person's first event | §6.1: missing item = version 0; `attribute_not_exists OR version` |
| 3 | Manifests and state were shared across environments | §3.2: `state/<env>/`, per-environment manifests and scale |
| 4 | LocalStack Firehose writes too many tiny objects | §3.4: incremental settle plus compaction; dbt reads `compacted/`; Day 1 measurement |

All non-blocking items N1–N9 from review 3 were also adopted.

**Review 4: 1 blocking issue, fixed in revision 5.** The `--local-out` path had no
persisted state, environment or manifest lifecycle, so versions restarted on live
runs. Fix: `ENV=sim` as a full third environment with a persisted sim store
(§3.2, §3.3). Non-blocking N1–N12 were also adopted:
- alerts are counted in settle;
- spool plus `make settle` for recovery;
- `control/poison/`;
- an exact glob per target;
- integration on a fresh stack;
- `make up` resets `state/local/`;
- AWS scale with people and properties scaled down too;
- Day 1 and Day 4 scale checks;
- golden-case ids;
- a one-hour skew margin on listing;
- reproducibility relative to the anchor time.

**Review 5: 2 blocking issues, fixed in revision 6.**

| # | Issue | Fix |
|---|---|---|
| 1 | `sim` had no location for reference data | `data/sim/ref/`, a per-target `ref_root`, `stg_properties`/`stg_people`/`stg_companies` |
| 2 | Poison runs were counted in the completeness manifests | Poison writes only `control/poison/` |

Non-blocking items also adopted:
- the integration and reset order;
- `reset ENV=sim`, refusing a second backfill, gitignore;
- settle counts only this run's ids, spool before cache;
- poison timing, Lambda timeout ≤ visibility;
- full Makefile list;
- Terraform state per environment, unique bucket;
- Dagster environment and DuckDB file per environment;
- live runs skip open visits.

**Review 6: 0 blocking issues.** All six non-blocking notes were folded into revision 6.1:
- AWS reset on destroy, and a manifest consistency check;
- `make settle` rebuilds ids from the manifest;
- `make up` does a full restart;
- live timing after the last visit end, and open visits;
- rule B source list, bucket `force_destroy`, poison timeout;
- absolute paths via `REPO_ROOT`, the sim DuckDB name, trace for sim.
