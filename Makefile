SHELL := /bin/bash
REPO_ROOT := $(CURDIR)
VENV := $(REPO_ROOT)/.venv
PY := $(VENV)/bin/python
DBT := $(VENV)/bin/dbt
export REPO_ROOT

ENV ?= sim
SCALE ?= ci
MODE ?= backfill
N ?= 50
# standdown_checker lives under lambdas/; the editable install does not map it.
GEN_PY := PYTHONPATH=$(REPO_ROOT)/lambdas $(PY)

.PHONY: venv fmt lint unit test setup generate reset dbt-build sim-data trace

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

setup:
	$(GEN_PY) -m generator setup --env $(ENV) --scale $(SCALE)

generate:
	$(GEN_PY) -m generator generate --env $(ENV) --mode $(MODE) --n $(N)

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
	$(MAKE) generate ENV=sim MODE=backfill
	$(MAKE) generate ENV=sim MODE=live N=30

HOPS ?= 3

trace:
	$(PY) -m tracing --db $(REPO_ROOT)/warehouse/$(ENV).duckdb --property $(PROPERTY) --start $(START) --max-hops $(HOPS)
