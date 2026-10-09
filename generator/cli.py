"""Command line for the generator: setup, generate, settle, poison, reset (spec §3.2, §8)."""

from __future__ import annotations

import argparse
import json
import random
import shutil
import sys
import time
import uuid
from datetime import UTC, datetime

from botocore.exceptions import BotoCoreError, ClientError
from standdown_checker.rules import load_rules

from . import aws as A
from . import queues as Q
from . import state as S
from .config import (
    DEFAULT_SCALE,
    ENVS,
    SCALES,
    compacted_dir,
    data_dir,
    ref_dir,
    rules_path,
    sim_store_path,
    state_dir,
    warehouse_path,
)
from .emit import emit
from .local_sim import InMemoryStore, run_sim
from .reference import Reference, build_reference
from .settle import TIMEOUT_SECONDS, Settler
from .stream import (
    BACKFILL_LATENESS,
    LIVE_LATENESS,
    Emission,
    build_emissions,
    event_id,
    plan_backfill,
    plan_live,
    update_cursors,
)

RUN_NAMESPACE = uuid.UUID("0b6c3f0e-58f4-4d0c-9d8a-1d6f1f0a7c21")
POISON_TIMEOUT_SECONDS = 300
DYNAMODB_BATCH = 25
sleep = time.sleep  # patched in tests


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _ts(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _load_properties(conn: A.Connection, ref: Reference) -> None:
    """Batch-write the snapshot to DynamoDB `properties`, resending unprocessed items."""
    requests = [
        {
            "PutRequest": {
                "Item": {
                    "property_id": {"S": p.property_id},
                    "property_type": {"S": p.property_type},
                    "owner_company_id": {"S": p.owner_company_id},
                }
            }
        }
        for p in ref.properties
    ]
    table = conn.outputs.property_table
    for start in range(0, len(requests), DYNAMODB_BATCH):
        pending = {table: requests[start : start + DYNAMODB_BATCH]}
        delay = 0.1
        for _ in range(10):
            pending = conn.dynamodb.batch_write_item(RequestItems=pending).get("UnprocessedItems")
            if not pending:
                break
            sleep(delay)
            delay *= 2
        else:
            raise S.GeneratorError("DynamoDB kept returning unprocessed property items")


def cmd_setup(args: argparse.Namespace) -> int:
    scale_name = args.scale or DEFAULT_SCALE[args.env]
    conn = A.connect(args.env) if args.env != "sim" else None
    if (ref_dir(args.env) / "properties.json").exists() or (conn and conn.keys("ref/")):
        raise S.GeneratorError(
            f"reference data already exists for ENV={args.env}; {S.reset_hint(args.env)}"
        )
    ref = build_reference(SCALES[scale_name], random.Random(args.seed))
    ref.write(ref_dir(args.env))
    if conn is not None:  # the same snapshot goes to S3 ref/ and DynamoDB (spec §3.6)
        for name in ("properties", "people", "companies"):
            body = (ref_dir(args.env) / f"{name}.json").read_bytes()
            conn.s3.put_object(Bucket=conn.outputs.bucket, Key=f"ref/{name}.json", Body=body)
        _load_properties(conn, ref)
    S.write_setup(args.env, args.seed, scale_name)
    print(f"setup ENV={args.env}: {len(ref.properties)} properties, {len(ref.people)} people")
    return 0


def cmd_generate(args: argparse.Namespace) -> int:
    setup = S.read_setup(args.env)
    state = S.load_state(args.env)
    S.check_consistency(args.env, state)
    conn = None
    if args.env != "sim":
        conn = A.connect(args.env)
        S.check_remote_consistency(args.env, state, conn.remote_manifest_ids())
    ref = Reference.read(ref_dir(args.env))
    scale = SCALES[setup["scale"]]
    seed = setup["seed"]

    if args.mode == "backfill":
        if state is not None:
            raise S.GeneratorError(
                f"ENV={args.env} already has generator state; a backfill runs once. "
                f"To start again, {S.reset_hint(args.env)}."
            )
        anchor = _ts(args.anchor) or _now()
        run_id = str(uuid.uuid5(RUN_NAMESPACE, f"{seed}:backfill:{anchor.isoformat()}"))
        rng = random.Random(f"{seed}:backfill")
        visits, plants = plan_backfill(ref, scale, rng, anchor)
        run_end, lateness, shortfall = anchor, BACKFILL_LATENESS, 0
        state = {
            "env": args.env,
            "seed": seed,
            "scale": setup["scale"],
            "anchor": anchor.isoformat(),
            "runs": [],
            "cursors": {},
        }
    else:
        if state is None:
            raise S.GeneratorError(
                f"no backfill yet for ENV={args.env}; "
                f"run `make generate ENV={args.env} MODE=backfill` first"
            )
        now = _ts(args.now) or _now()
        run_id = str(uuid.uuid4())
        rng = random.Random(f"{seed}:{run_id}")
        visits, shortfall = plan_live(
            ref, state["cursors"], rng, now, args.n, scale.max_checkins_per_person
        )
        plants, run_end, lateness = [], now, LIVE_LATENESS

    emissions, counts = build_emissions(visits, plants, ref, run_id, rng, run_end, lateness)
    if conn is None:
        S.write_manifest(args.env, run_id, args.mode, seed, counts)  # before "emitting" (§3.2)
        properties = {
            p.property_id: {
                "property_type": p.property_type,
                "owner_company_id": p.owner_company_id,
            }
            for p in ref.properties
        }
        store = InMemoryStore.load(sim_store_path(), properties)
        out_dir = compacted_dir(args.env) / run_id
        result = run_sim(emissions, store, load_rules(rules_path()), out_dir)
        store.save(sim_store_path())
    else:
        manifest = S.write_manifest(
            args.env, run_id, args.mode, seed, counts, started_at=_now().isoformat()
        )
        conn.put_json(f"control/manifests/{run_id}.json", manifest)  # before emitting (§3.2)
        result = {"sent": _emit(args.env, emissions, conn)}
    update_cursors(state["cursors"], visits)
    state["runs"].append(run_id)
    S.save_state(args.env, state)
    anchor_note = f" anchor={state['anchor']}" if args.mode == "backfill" else ""
    print(f"{args.mode} run {run_id}{anchor_note}: {counts} -> {result}")
    if shortfall:
        print(f"note: {shortfall} requested check-ins did not fit (no one had room)")
    if conn is not None and not args.no_settle:
        settle_run(conn, manifest, args.timeout)
    return 0


def _emit(env: str, emissions: list[Emission], conn: A.Connection) -> int:
    """Emit after the control record is written; a failure leaves state that needs a reset."""
    try:
        return emit(emissions, conn.events, conn.outputs.bus_name)
    except (S.GeneratorError, BotoCoreError, ClientError) as exc:
        raise S.GeneratorError(f"{exc}; {S.reset_hint(env)}") from None


def settle_run(conn: A.Connection, manifest: dict, timeout: float) -> dict:
    started = datetime.now(UTC)
    try:
        settled = Settler(manifest, conn, timeout_seconds=timeout).run()
    except (BotoCoreError, ClientError) as exc:  # the spool survives; a rerun resumes from it
        raise S.GeneratorError(
            f"{exc}; settling stopped and the spool is kept: once the stack is reachable, resume "
            f"with `make settle ENV={conn.env} RUN={manifest['run_id']}`"
        ) from None
    seconds = (datetime.now(UTC) - started).total_seconds()
    print(
        f"settled run {manifest['run_id']} in {seconds:.0f}s: {settled['envelopes']} envelopes, "
        f"{settled['breaches']} breaches -> {', '.join(settled['parts'])}"
    )
    return settled


def cmd_settle(args: argparse.Namespace) -> int:
    if args.env == "sim":
        raise S.GeneratorError("ENV=sim needs no settling; its runs are written complete")
    manifest = S.read_manifest(args.env, args.run)
    settle_run(A.connect(args.env), manifest, args.timeout)
    return 0


LAST_ALERTS = 5


def _detected(record: dict) -> datetime | None:
    """The alert's detected time, if it is a valid ISO 8601 time with an offset."""
    try:
        detected = datetime.fromisoformat(record["detected_time"])
    except (KeyError, TypeError, ValueError):
        return None
    return detected if detected.tzinfo is not None else None  # naive cannot sort with aware


def cmd_alerts(args: argparse.Namespace) -> int:
    """Count the stand-down alerts in the checker Lambda's logs; show the latest (spec §9)."""
    if args.env == "sim":
        raise S.GeneratorError("alerts needs ENV=local or ENV=aws (it reads Lambda logs)")
    outputs = A.load_outputs(args.env)
    logs = A.client(args.env, "logs", outputs.region)
    log_group = f"/aws/lambda/{outputs.standdown_function}"  # Terraform creates it beforehand
    lines = 0
    distinct: dict[str, dict] = {}
    token = None
    while True:
        kwargs = {"logGroupName": log_group, "filterPattern": '{ $.level = "alert" }'}
        if token:
            kwargs["nextToken"] = token
        page = logs.filter_log_events(**kwargs)
        for event in page.get("events", []):
            try:  # LocalStack ignores filterPattern, so always filter here
                record = json.loads(event["message"])
            except ValueError:
                continue
            if isinstance(record, dict) and record.get("level") == "alert":
                lines += 1
                distinct.setdefault(str(record.get("checkin_event_id")), record)
        token = page.get("nextToken")
        if not token:
            break
    print(
        f"{lines} alert log lines, {len(distinct)} distinct alerts "
        "(by checkin_event_id; repeats are redeliveries or duplicate events)"
    )
    timed = [(t, r) for r in distinct.values() if (t := _detected(r)) is not None]
    latest = sorted(timed, key=lambda tr: (tr[0], str(tr[1].get("checkin_event_id"))))
    if latest:
        print(
            f"last {min(LAST_ALERTS, len(latest))} alerts (detected time, person, origin -> "
            "destination property, rule):"
        )
    for _, r in latest[-LAST_ALERTS:]:
        print(
            f"  {r['detected_time']}  {r.get('person_id')}  "
            f"{r.get('origin_property_id')} -> {r.get('dest_property_id')}  {r.get('rule_id')}"
        )
    return 0


def cmd_poison(args: argparse.Namespace) -> int:
    if args.env == "sim":
        raise S.GeneratorError("poison needs ENV=local or ENV=aws (it demonstrates the DLQ)")
    conn = A.connect(args.env)
    conn.keys("control/")  # a lost stack fails here, before any local record is written
    run_id = str(uuid.uuid4())
    sent_at = _now()
    record = {"run_id": run_id, "k": args.k, "sent_at": sent_at.isoformat()}
    conn.put_json(f"control/poison/{run_id}.json", record)  # never control/manifests/
    S.write_json(state_dir(args.env) / "poison" / f"{run_id}.json", record)
    poison = [
        Emission(sent_at, "CheckIn", {"poison": True, "event_id": event_id(run_id, seq)})
        for seq in range(args.k)
    ]
    _emit(args.env, poison, conn)
    expected = S.poison_injected(args.env)
    Q.wait_for_dlq(conn.sqs, conn.outputs.visit_dlq_url, expected, timeout=args.timeout)
    return 0


def cmd_reset(args: argparse.Namespace) -> int:
    if args.env == "local":
        raise S.GeneratorError(
            "reset does not apply to ENV=local; use `make up` "
            "(it recreates LocalStack and resets state/local)"
        )
    if args.env == "aws":
        raise S.GeneratorError(
            "reset does not apply to ENV=aws (the AWS resources would remain); "
            "use `make destroy ENV=aws`"
        )
    shutil.rmtree(state_dir(args.env), ignore_errors=True)
    shutil.rmtree(data_dir(args.env), ignore_errors=True)
    warehouse_path(args.env).unlink(missing_ok=True)
    print(f"reset ENV={args.env}")
    return 0


COMMANDS = {
    "setup": cmd_setup,
    "generate": cmd_generate,
    "settle": cmd_settle,
    "poison": cmd_poison,
    "alerts": cmd_alerts,
    "reset": cmd_reset,
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="generator")
    sub = parser.add_subparsers(dest="command", required=True)
    p_setup = sub.add_parser("setup")
    p_setup.add_argument("--env", choices=ENVS, required=True)
    p_setup.add_argument("--scale", choices=sorted(SCALES), help="default: per environment")
    p_setup.add_argument("--seed", type=int, default=42)
    p_gen = sub.add_parser("generate")
    p_gen.add_argument("--env", choices=ENVS, required=True)
    p_gen.add_argument("--mode", choices=("backfill", "live"), required=True)
    p_gen.add_argument("--n", type=int, default=50)
    p_gen.add_argument("--anchor", help="backfill end time (ISO 8601); default now")
    p_gen.add_argument("--now", help="live run time (ISO 8601); default now")
    p_gen.add_argument("--no-settle", action="store_true", help="local/aws: emit only")
    p_gen.add_argument("--timeout", type=float, default=TIMEOUT_SECONDS)
    p_settle = sub.add_parser("settle")
    p_settle.add_argument("--env", choices=ENVS, required=True)
    p_settle.add_argument("--run", required=True)
    p_settle.add_argument("--timeout", type=float, default=TIMEOUT_SECONDS)
    p_poison = sub.add_parser("poison")
    p_poison.add_argument("--env", choices=ENVS, required=True)
    p_poison.add_argument("--k", type=int, default=3)
    p_poison.add_argument("--timeout", type=float, default=POISON_TIMEOUT_SECONDS)
    p_alerts = sub.add_parser("alerts")
    p_alerts.add_argument("--env", choices=ENVS, required=True)
    p_reset = sub.add_parser("reset")
    p_reset.add_argument("--env", choices=ENVS, required=True)
    args = parser.parse_args(argv)
    try:
        return COMMANDS[args.command](args)
    except S.GeneratorError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except (BotoCoreError, ClientError) as exc:  # an AWS call outside the handled paths
        print(f"error: {exc}; {S.reset_hint(args.env)}", file=sys.stderr)
        return 2
