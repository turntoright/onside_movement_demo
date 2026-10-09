"""Command line for the generator: setup, generate, reset (spec §3.2, §8)."""

from __future__ import annotations

import argparse
import random
import shutil
import sys
import uuid
from datetime import UTC, datetime

from standdown_checker.rules import load_rules

from . import state as S
from .config import (
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
from .local_sim import InMemoryStore, run_sim
from .reference import Reference, build_reference
from .stream import (
    BACKFILL_LATENESS,
    LIVE_LATENESS,
    build_emissions,
    plan_backfill,
    plan_live,
    update_cursors,
)

RUN_NAMESPACE = uuid.UUID("0b6c3f0e-58f4-4d0c-9d8a-1d6f1f0a7c21")


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _ts(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def cmd_setup(args: argparse.Namespace) -> int:
    if (ref_dir(args.env) / "properties.json").exists():
        raise S.GeneratorError(
            f"reference data already exists for ENV={args.env}; "
            f"run `make reset ENV={args.env}` first"
        )
    ref = build_reference(SCALES[args.scale], random.Random(args.seed))
    ref.write(ref_dir(args.env))
    S.write_setup(args.env, args.seed, args.scale)
    print(f"setup ENV={args.env}: {len(ref.properties)} properties, {len(ref.people)} people")
    return 0


def cmd_generate(args: argparse.Namespace) -> int:
    setup = S.read_setup(args.env)
    state = S.load_state(args.env)
    S.check_consistency(args.env, state)
    ref = Reference.read(ref_dir(args.env))
    scale = SCALES[setup["scale"]]
    seed = setup["seed"]

    if args.mode == "backfill":
        if state is not None:
            raise S.GeneratorError(
                f"ENV={args.env} already has generator state; a backfill runs once. "
                f"Run `make reset ENV={args.env}` to start again."
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
    S.write_manifest(args.env, run_id, args.mode, seed, counts)  # before "emitting" (§3.2)
    properties = {
        p.property_id: {"property_type": p.property_type, "owner_company_id": p.owner_company_id}
        for p in ref.properties
    }
    store = InMemoryStore.load(sim_store_path(), properties)
    derived = run_sim(emissions, store, load_rules(rules_path()), compacted_dir(args.env) / run_id)
    store.save(sim_store_path())
    update_cursors(state["cursors"], visits)
    state["runs"].append(run_id)
    S.save_state(args.env, state)
    anchor_note = f" anchor={state['anchor']}" if args.mode == "backfill" else ""
    print(f"{args.mode} run {run_id}{anchor_note}: {counts} -> {derived}")
    if shortfall:
        print(f"note: {shortfall} requested check-ins did not fit (no one had room)")
    return 0


def cmd_reset(args: argparse.Namespace) -> int:
    shutil.rmtree(state_dir(args.env), ignore_errors=True)
    shutil.rmtree(data_dir(args.env), ignore_errors=True)
    warehouse_path(args.env).unlink(missing_ok=True)
    print(f"reset ENV={args.env}")
    return 0


COMMANDS = {"setup": cmd_setup, "generate": cmd_generate, "reset": cmd_reset}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="generator")
    sub = parser.add_subparsers(dest="command", required=True)
    p_setup = sub.add_parser("setup")
    p_setup.add_argument("--env", choices=ENVS, required=True)
    p_setup.add_argument("--scale", choices=sorted(SCALES), default="ci")
    p_setup.add_argument("--seed", type=int, default=42)
    p_gen = sub.add_parser("generate")
    p_gen.add_argument("--env", choices=ENVS, required=True)
    p_gen.add_argument("--mode", choices=("backfill", "live"), required=True)
    p_gen.add_argument("--n", type=int, default=50)
    p_gen.add_argument("--anchor", help="backfill end time (ISO 8601); default now")
    p_gen.add_argument("--now", help="live run time (ISO 8601); default now")
    p_reset = sub.add_parser("reset")
    p_reset.add_argument("--env", choices=ENVS, required=True)
    args = parser.parse_args(argv)
    try:
        return COMMANDS[args.command](args)
    except S.GeneratorError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
