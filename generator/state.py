"""Generator state and manifests under state/<env>/ (spec §3.2)."""

from __future__ import annotations

import json
from pathlib import Path

from .config import state_dir


class GeneratorError(RuntimeError):
    """A user-facing error whose message says how to fix it."""


def write_json(path: Path, obj: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=1, sort_keys=True))
    tmp.replace(path)


def read_setup(env: str) -> dict:
    path = state_dir(env) / "setup.json"
    if not path.exists():
        raise GeneratorError(f"no setup for ENV={env}; run `make setup ENV={env}` first")
    return json.loads(path.read_text())


def write_setup(env: str, seed: int, scale_name: str) -> None:
    write_json(state_dir(env) / "setup.json", {"seed": seed, "scale": scale_name})


def load_state(env: str) -> dict | None:
    path = state_dir(env) / "generator_state.json"
    return json.loads(path.read_text()) if path.exists() else None


def save_state(env: str, state: dict) -> None:
    write_json(state_dir(env) / "generator_state.json", state)


def reset_hint(env: str) -> str:
    """How to start an environment again (spec §3.2)."""
    if env == "local":
        return "run `make up` (LocalStack restarts empty), then deploy and setup again"
    if env == "aws":
        return "run `make destroy ENV=aws`, then deploy and setup again"
    return f"run `make reset ENV={env}` and start again"


def write_manifest(
    env: str,
    run_id: str,
    mode: str,
    seed: int,
    counts: dict[str, int],
    started_at: str | None = None,
) -> dict:
    manifest = {
        "run_id": run_id,
        "mode": mode,
        "seed": seed,
        "env": env,
        "counts": counts,
        "total": sum(counts.values()),
        "started_at": started_at,  # wall clock (UTC) before emitting; None for sim
    }
    write_json(state_dir(env) / "manifests" / f"{run_id}.json", manifest)
    return manifest


def read_manifest(env: str, run_id: str) -> dict:
    path = state_dir(env) / "manifests" / f"{run_id}.json"
    if not path.exists():
        raise GeneratorError(f"no manifest for run {run_id!r} in state/{env}/manifests")
    return json.loads(path.read_text())


def manifest_ids(env: str) -> set[str]:
    directory = state_dir(env) / "manifests"
    return {p.stem for p in directory.glob("*.json")} if directory.exists() else set()


def check_consistency(env: str, state: dict | None) -> None:
    runs = set(state["runs"]) if state else set()
    found = manifest_ids(env)
    if found != runs:
        raise GeneratorError(
            f"ENV={env} manifests {sorted(found)} do not match generator state {sorted(runs)}; "
            f"{reset_hint(env)}"
        )


def check_remote_consistency(env: str, state: dict | None, remote_ids: set[str]) -> None:
    """S3 control/manifests/ must match state/<env>/ (spec §3.2), e.g. after a lost LocalStack."""
    runs = set(state["runs"]) if state else set()
    if remote_ids != runs:
        raise GeneratorError(
            f"ENV={env}: S3 control/manifests {sorted(remote_ids)} do not match "
            f"state/{env} runs {sorted(runs)}; {reset_hint(env)}"
        )
