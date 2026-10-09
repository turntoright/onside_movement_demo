"""Scales and paths (spec §3.2). Every path hangs off REPO_ROOT so dbt and tests agree."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

ENVS = ("sim",)  # Plan 2 adds "local" and "aws"


@dataclass(frozen=True)
class Scale:
    name: str
    n_properties: int
    n_people: int
    n_owner_companies: int
    n_visitor_companies: int
    n_checkins: int
    max_checkins_per_person: int = 300


SCALES = {
    "default": Scale("default", 500, 2000, 120, 40, 100_000),
    "aws": Scale("aws", 40, 100, 12, 6, 3_000),
    "ci": Scale("ci", 80, 200, 20, 8, 5_000),
}


def repo_root() -> Path:
    return Path(os.environ.get("REPO_ROOT", Path.cwd())).resolve()


def state_dir(env: str) -> Path:
    return repo_root() / "state" / env


def data_dir(env: str) -> Path:
    return repo_root() / "data" / env


def ref_dir(env: str) -> Path:
    return data_dir(env) / "ref"


def compacted_dir(env: str) -> Path:
    return data_dir(env) / "compacted"


def rules_path() -> Path:
    return repo_root() / "shared" / "standdown_rules.csv"


def sim_store_path() -> Path:
    return state_dir("sim") / "sim_store.json"


def warehouse_path(env: str) -> Path:
    return repo_root() / "warehouse" / f"{env}.duckdb"
