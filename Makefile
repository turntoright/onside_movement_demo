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

.PHONY: venv fmt lint unit test setup generate reset

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
