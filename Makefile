SHELL := /bin/bash
REPO_ROOT := $(CURDIR)
VENV := $(REPO_ROOT)/.venv
PY := $(VENV)/bin/python
DBT := $(VENV)/bin/dbt
export REPO_ROOT

# Only a command-line ENV overrides this (a shell variable named ENV never does).
ENV = sim
# Empty SCALE means the per-environment default (sim: ci, local: local, aws: aws).
SCALE =
MODE = backfill
N = 50
K = 3
RUN =
# Optional settle/poison timeout in seconds; passed only when set.
TIMEOUT =
# Optional pins for reproducible sim data (ISO 8601); passed only when set.
ANCHOR ?=
NOW ?=
# standdown_checker lives under lambdas/; the editable install does not map it.
# Unbuffered, so emit and settle progress shows up while a long run is going.
GEN_PY := PYTHONUNBUFFERED=1 PYTHONPATH=$(REPO_ROOT)/lambdas $(PY)

.PHONY: venv fmt lint unit test setup generate settle poison reset dbt-build sim-data \
	sim-e2e trace

venv:
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install --upgrade pip
	$(VENV)/bin/pip install -e '.[dev]'

fmt:
	$(VENV)/bin/ruff format .
	$(VENV)/bin/ruff check --fix .

lint:
	$(VENV)/bin/ruff check .
	$(VENV)/bin/ruff format --check .

unit:
	$(VENV)/bin/pytest -q

test: lint unit

# ---- data (ENV=sim|local|aws) ----

setup:
	$(GEN_PY) -m generator setup --env $(ENV) $(if $(SCALE),--scale $(SCALE))

generate:
	$(GEN_PY) -m generator generate --env $(ENV) --mode $(MODE) --n $(N) \
		$(if $(ANCHOR),--anchor $(ANCHOR)) $(if $(NOW),--now $(NOW)) \
		$(if $(TIMEOUT),--timeout $(TIMEOUT))

settle:
	$(if $(RUN),,$(error settle needs RUN=<run_id>, see state/$(ENV)/manifests/))
	$(GEN_PY) -m generator settle --env $(ENV) --run $(RUN) $(if $(TIMEOUT),--timeout $(TIMEOUT))

poison:
	$(GEN_PY) -m generator poison --env $(ENV) --k $(K) $(if $(TIMEOUT),--timeout $(TIMEOUT))

reset:
	$(GEN_PY) -m generator reset --env $(ENV)

DBT_ENV = DBT_PROFILES_DIR=$(REPO_ROOT)/dbt \
	DBT_COMPACTED_GLOB='$(REPO_ROOT)/data/$(ENV)/compacted/*/*.ndjson' \
	DBT_REF_ROOT='$(REPO_ROOT)/data/$(ENV)/ref/'

dbt-build:
	mkdir -p $(REPO_ROOT)/warehouse
	cd dbt && $(DBT_ENV) $(DBT) build --target $(ENV)

sim-data:
	$(MAKE) reset ENV=sim
	$(MAKE) setup ENV=sim SCALE=ci
	$(MAKE) generate ENV=sim MODE=backfill ANCHOR=$(ANCHOR)
	$(MAKE) generate ENV=sim MODE=live N=30 NOW=$(NOW)

sim-e2e: sim-data
	$(MAKE) dbt-build ENV=sim

HOPS ?= 3

trace:
	$(PY) -m tracing --db $(REPO_ROOT)/warehouse/$(ENV).duckdb --property $(PROPERTY) --start $(START) --max-hops $(HOPS)
