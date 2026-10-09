# Onside movement demo — design

Date: 2026-10-09. Status: approved in conversation, awaiting written-spec review.

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
- AWS event chain: EventBridge, SQS FIFO + DLQ, Lambda, DynamoDB, Firehose, S3.
- Terraform for all infrastructure, one codebase for LocalStack and AWS.
- Dagster orchestration with dagster-dbt.
- dbt with the DuckDB adapter, reading S3.
- Synthetic data generator.

### Out of scope (named in the README as "production next steps")
- API Gateway / webhook ingestion (the generator plays the app backend).
- ClickHouse, Iceberg, Athena, BI dashboards.
- Incremental dbt models (full rebuild at demo scale).

## 2. Architecture

```
 Generator (Python)
        │ PutEvents: CheckIn / CheckOut / PlantMovement
        ▼
 EventBridge bus: onside-events
   ├─ rule CheckIn|CheckOut ──► SQS FIFO visit-queue (group = person_id, DLQ after 3)
   │                     ──► Lambda standdown_checker
   │                           ├─ DynamoDB processed_events     (dedupe by event_id, TTL 7d)
   │                           ├─ DynamoDB person_recent_visits (14-day visit list, versioned)
   │                           └─ on breach: PutEvents HighRiskVisitDetected ─┐
   └─ rule all events (incl. HighRiskVisitDetected) ◄──────────────────────────┘
          ──► Firehose ──► S3 raw/ (JSON lines, date-partitioned)
                                │
 Dagster (schedule + S3 sensor)
   └─ dbt-duckdb reads S3 → models → tests → DuckDB file
        └─ outputs: movement network, high-risk visits, alert reconciliation,
                    key properties, tracing
```

### Components

| Unit | Responsibility | Depends on |
|---|---|---|
| `generator/` | Builds reference data and an event stream; emits to EventBridge; writes reference data and a manifest to S3 | boto3 |
| `lambdas/standdown_checker/rules.py` | Pure stand-down logic: given a new visit, the person's recent visits and the rule matrix, return breaches | stdlib only |
| `lambdas/standdown_checker/state.py` | DynamoDB access: dedupe, read/write the versioned visit list | boto3 |
| `lambdas/standdown_checker/handler.py` | SQS batch handler: validate (pydantic), dedupe, update state, evaluate, emit alerts, report partial batch failures | rules, state |
| `shared/standdown_rules.csv` | The single source of stand-down rules; bundled into the Lambda zip and used as a dbt seed | — |
| `infra/terraform/` | All AWS resources; a variable switches between LocalStack endpoints and real AWS | — |
| `dbt/` | Staging → intermediate → marts, tests, Python model for network metrics | DuckDB, S3 |
| `orchestration/` | Dagster definitions: dbt assets, S3 sensor, asset checks, trace job | dbt, boto3 |

## 3. Data

### Generator behaviour
Fixed random seed, so output is reproducible. Default scale: about 500 properties,
2,000 people, 100,000 check-ins over 12 months.

- Properties have a type (e.g. poultry, pork, beef, dairy, kiwifruit, vineyard, office,
  feed mill), group, owning company, region and coordinates.
- About 70% of moves between properties are under 10 km, with rare long moves (from
  the paper's 71.3%).
- About 5% of check-ins are offline: received hours to days after `event_time`.
- About 2% of events are duplicated.
- Some visits have no check-out (auto check-out is inferred downstream).
- Group check-ins carry `additional_visitors`.
- It writes `manifest.json`: the count of distinct event_ids emitted, by type.

### Events (`detail` of each EventBridge event)

| Event | Fields |
|---|---|
| `CheckIn` | event_id, person_id, company_id, property_id, event_time, received_time, method (app, geofence, qr, kiosk, link), job_type, additional_visitors, answers[] |
| `CheckOut` | event_id, checkin_event_id, person_id, property_id, event_time, received_time, method (manual, geofence_auto, end_of_day) |
| `PlantMovement` | event_id, source_property_id, dest_property_id, movement_date, material_type, volume, received_time |
| `HighRiskVisitDetected` | event_id, checkin_event_id, person_id, origin_property_id, dest_property_id, nights_since, required_nights, rule_id, detected_time |

Reference data in S3 `ref/`: properties, people, companies. The rules come from
`shared/standdown_rules.csv`, with columns rule_id, from_type, to_type, scope
(internal, external, any) and nights.

## 4. Stand-down logic (shared by Lambda and dbt)

- A **night** is a local midnight in `Pacific/Auckland`. `nights_between` counts the
  local midnights between the earlier visit's **end** and the new check-in. It is not a
  difference in hours divided by 24, so daylight-saving days stay correct.
- A visit's **end** is its actual check-out time if a CheckOut event exists, otherwise
  its check-in time. Inferred check-outs (§5) are **not** used here, so the Lambda and
  dbt agree. A CheckOut that arrives after a later check-in was evaluated can only make
  the realtime side miss a breach, never invent one.
- A new check-in at property D breaches a rule when **any** visit by the same person in
  the last 14 days, at property O ≠ D, matches a rule (O.type → D.type, with scope
  internal if both are owned by the same company, otherwise external) and
  `nights_between < nights`.
- If several rules match, the one with the largest `nights` wins.
- This differs on purpose from the movement definition (§5), which looks only at the
  previous visit. Example: poultry A on the 3rd, office B on the 4th, poultry C on the
  4th. The movement is B→C, but A→C breaches the 3-night poultry rule.

## 5. dbt models

- **staging** (`stg_checkins`, `stg_checkouts`, `stg_plant_movements`,
  `stg_realtime_alerts`): read raw JSON from S3, deduplicate on event_id, cast types,
  convert to NZ time.
- **intermediate** (`int_visits`): pair each check-in with its check-out. If it is
  missing, infer it from the person's next check-in, capped at the end of that local
  day. Record the method in `checkout_source`.
- **marts:**
  - `fct_movements`: each person's consecutive visits ordered by `event_time`, at
    different properties (no self-loops). Columns: origin, destination, gap_hours,
    nights_between, distance_km (haversine), internal/external, month, and `connected`
    (gap under `var('connected_window_days')`, default 7).
  - `fct_high_risk_visits`: breaches as defined in §4.
  - `fct_alert_reconciliation`: full outer join of the batch breaches with the realtime
    alerts on checkin_event_id. Each row is classified as in §6.
  - `fct_network_edges_monthly`: origin → destination per month. The weight is the
    average daily movements, as in the paper.
  - `dim_property_network` (dbt Python model, networkx): in-degree, out-degree,
    weighted strength, PageRank. This is the demo's "key property identification".
- `stg_plant_movements` is used only by tracing, as a second edge type
  (`edge_type = plant_material`) next to people movements.
- **Tracing** (Dagster job, configured with property_id, start_date, max_hops): a
  recursive CTE over `fct_movements` that only follows **time-respecting paths**, where
  each hop departs after the previous hop arrived. It outputs the downstream properties
  with their paths and arrival times.

All models are rebuilt in full. The README explains the production alternative:
incremental by received_time, a lookback window, and recomputing the affected people.

## 6. Error handling and data quality

### Lambda
- **Dedupe:** a conditional put (`attribute_not_exists(event_id)`) into
  `processed_events`, with a TTL. Duplicates are skipped and counted as
  `DuplicateSkipped`.
- **Ordering:** the FIFO queue groups messages by person_id, so online events arrive in
  order. Only offline (late) events arrive out of order.
- **State:** `person_recent_visits` keeps one item per person, holding a list of visits
  inside the 14-day window. Writes use optimistic locking on a version attribute and
  retry on conflict.
- **CheckOut events** update `end` on the matching visit in `person_recent_visits`.
- **Late events:** a late event is inserted at its `event_time` position and checked
  against the history before it.
- **Known limitation (kept on purpose):** check-ins that were already processed are
  **not** re-checked when an earlier visit arrives late. Batch reconciliation finds
  these.
- **Bad messages:** a pydantic validation failure goes into `batchItemFailures`. After
  3 receives the message moves to the DLQ.

### Firehose
Firehose delivers at least once, so S3 may hold duplicates. Staging deduplicates again.

### Reconciliation classes (`fct_alert_reconciliation`)

| Class | Meaning | Expectation |
|---|---|---|
| `matched` | Both sides flagged it | Most rows |
| `matched` with flag `late_alert` | Flagged, but `detected_time − event_time` > 1 hour | Some |
| `missed_by_realtime` with cause `late_origin` | The breaching earlier visit arrived after this check-in was processed | Some, in line with the offline rate |
| `missed_by_realtime` with cause `late_checkout` | The earlier visit's CheckOut arrived after this check-in was processed | Few |
| `false_alert_by_realtime` | Realtime flagged it but batch did not | **Must be 0** |

### Tests
- **Generic dbt tests:** unique and not-null on event_id; relationships for property_id
  and person_id; accepted values for method.
- **Singular dbt tests:** check-out ≥ check-in; movement origin ≠ destination;
  nights_between ≥ 0.
- **dbt unit tests:**
  - nights_between across a daylight-saving change;
  - a stand-down breach with a visit in between;
  - an inferred check-out;
  - a late visit put back in order.
- **Source freshness** on received_time.
- **Reconciliation tests:** false_alert = 0 (error); missed rate above a threshold (warn).

### Dagster asset checks
- **Completeness:** distinct event_ids in S3 equal the generator manifest.
- **DLQ empty:** checks the queue depth through boto3.
- **Freshness:** the latest received_time is within the expected window.

## 7. Testing strategy

1. **Unit (pytest, TDD):**
   - `rules.py` cases: daylight saving, an intervening visit, scope, the largest-nights
     tie-break, a late insert.
   - `handler.py` with moto: dedupe, a version conflict with retry, a partial batch
     failure.
2. **dbt:** the unit and data tests in §6. In CI the dbt target reads local files
   instead of S3.
3. **Integration:** full chain on LocalStack. Send a small fixed event set, then assert
   the DynamoDB state, the S3 objects and the emitted alerts.
4. **Parity:** `false_alert_by_realtime = 0` over the full generated dataset shows that
   the Lambda and dbt logic agree.

## 8. CI/CD and deployment

- **GitHub Actions:**
  - ruff, pytest, `terraform fmt -check` and `terraform validate`, and `dbt build` on a
    small generated dataset.
  - A separate job runs the LocalStack integration test.
- **Makefile:** `up`, `deploy ENV=local|aws`, `generate`, `dagster`,
  `trace PROPERTY=… DATE=…`, `destroy ENV=…`, `test`.
- **Real AWS:**
  - Region ap-southeast-2, local Terraform state.
  - A $5 budget alarm is created first; `make destroy` runs after the screenshots,
    which are stored in `docs/`.
  - Lambda is packaged with `archive_file`.
- **Day-1 risk:** the LocalStack community image may now need an auth token. If it is
  unusable, fall back to moto server mode for the same services. The architecture stays
  the same.

## 9. Demo script (about 5 minutes)

1. The README architecture and how it maps to Onside: check-ins, stand-down, Tracing,
   "Key property identification" (30 s).
2. `make generate`: events flow and HighRiskVisit alerts appear in the logs (1 min).
3. Dagster UI: lineage, a run, green checks (1 min).
4. Reconciliation: "X breaches were missed in real time because of offline sync. That's
   why the warehouse recomputes on event_time." (1.5 min)
5. `make trace`: downstream properties from an infected property, then the PageRank
   key properties (1 min).
6. Production next steps: incremental models with a lookback, Iceberg + Athena,
   ClickHouse for customer-facing reports, data residency (30 s).

## 10. Schedule

| Day | Work |
|---|---|
| D1 | Repo, LocalStack check, generator, `rules.py` with tests |
| D2 | Terraform, Lambda, LocalStack integration test |
| D3 | dbt models and tests, including reconciliation |
| D4 | Dagster (assets, checks, trace job), CI |
| D5 | Real AWS deploy, README, demo rehearsal |
