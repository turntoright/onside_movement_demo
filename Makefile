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
# `or` rather than `?=`: an empty shell variable must not produce an empty endpoint.
LOCALSTACK_ENDPOINT := $(or $(LOCALSTACK_ENDPOINT),http://localhost:4566)
# Optional pins for reproducible sim data (ISO 8601); passed only when set.
ANCHOR ?=
NOW ?=
# standdown_checker lives under lambdas/; the editable install does not map it.
# Unbuffered, so emit and settle progress shows up while a long run is going.
GEN_PY := PYTHONUNBUFFERED=1 PYTHONPATH=$(REPO_ROOT)/lambdas $(PY)

.PHONY: venv fmt lint unit test setup generate settle poison reset dbt-build sim-data \
	sim-e2e trace alerts

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

# dbt reads local files for sim and S3 for local/aws (spec §3.3 table); the bucket comes
# from state/<env>/outputs.json, written by `make deploy`.
ifeq ($(ENV),sim)
DATA_GLOB = $(REPO_ROOT)/data/sim/compacted/*/*.ndjson
REF_ROOT = $(REPO_ROOT)/data/sim/ref/
else
BUCKET = $(shell $(PY) -c 'import json, sys; print(json.load(open(sys.argv[1]))["bucket"]["value"])' \
	$(REPO_ROOT)/state/$(ENV)/outputs.json 2>/dev/null)
DATA_GLOB = s3://$(BUCKET)/compacted/$(ENV)/*/*.ndjson
REF_ROOT = s3://$(BUCKET)/ref/
endif

# LOCALSTACK_S3_HOST (host:port, no scheme) feeds the dbt `local` profile's S3 secret.
DBT_ENV = DBT_PROFILES_DIR=$(REPO_ROOT)/dbt \
	LOCALSTACK_S3_HOST='$(patsubst http://%,%,$(LOCALSTACK_ENDPOINT))' \
	DBT_COMPACTED_GLOB='$(DATA_GLOB)' \
	DBT_REF_ROOT='$(REF_ROOT)'

dbt-build:
	@if [ "$(ENV)" != sim ] && [ -z "$(BUCKET)" ]; then \
		echo "error: no bucket in state/$(ENV)/outputs.json; run make deploy ENV=$(ENV)" >&2; exit 2; fi
	mkdir -p $(REPO_ROOT)/warehouse
	cd dbt && $(DBT_ENV) $(DBT) build --target $(ENV)

# Spec §9 step 2: stand-down alerts show up in the checker Lambda's logs.
alerts:
	$(GEN_PY) -m generator alerts --env $(ENV)

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

# ---- LocalStack (ENV=local) ----

LAMBDA_IMAGE := public.ecr.aws/lambda/python:3.12

.PHONY: up down package

# A full restart: LocalStack keeps nothing, so local state and Terraform state go too.
# The Lambda image is pulled only when missing so the demo also runs offline.
up:
	docker compose down -v --remove-orphans
	rm -rf $(REPO_ROOT)/state/local $(REPO_ROOT)/data/local $(REPO_ROOT)/warehouse/local.duckdb
	rm -f $(REPO_ROOT)/infra/terraform/state/local.tfstate $(REPO_ROOT)/infra/terraform/state/local.tfstate.backup
	docker compose up -d --wait
	docker image inspect $(LAMBDA_IMAGE) >/dev/null 2>&1 || docker pull -q $(LAMBDA_IMAGE)

down:
	docker compose down -v --remove-orphans

# Lambda bundles: the package plus, for the checker, the rules CSV next to it and the
# pure-Python tzdata copied from the venv (no network). boto3 comes from the Lambda runtime.
package:
	rm -rf $(REPO_ROOT)/build/standdown_checker $(REPO_ROOT)/build/archiver
	mkdir -p $(REPO_ROOT)/build/standdown_checker $(REPO_ROOT)/build/archiver
	cp -r $(REPO_ROOT)/lambdas/standdown_checker $(REPO_ROOT)/build/standdown_checker/
	cp $(REPO_ROOT)/shared/standdown_rules.csv $(REPO_ROOT)/build/standdown_checker/
	cp -r "$$($(PY) -c 'import tzdata, os; print(os.path.dirname(tzdata.__file__))')" $(REPO_ROOT)/build/standdown_checker/
	cp -r $(REPO_ROOT)/lambdas/archiver $(REPO_ROOT)/build/archiver/
	find $(REPO_ROOT)/build -name __pycache__ -type d -prune -exec rm -rf {} +

# ---- infrastructure (ENV=local|aws, always explicit) ----

# ENV=aws only: the 12-digit account Terraform may touch (make deploy ENV=aws AWS_ACCOUNT_ID=...).
AWS_ACCOUNT_ID ?=
TF_DIR := $(REPO_ROOT)/infra/terraform
# One data directory per environment (gitignored), so switching ENV never re-initialises and
# an initialised environment needs no network: `terraform init` always queries the registry.
TF_DATA = $(TF_DIR)/.terraform-$(ENV)
TF = CHECKPOINT_DISABLE=1 TF_DATA_DIR=$(TF_DATA) terraform -chdir=$(TF_DIR)
# One local state file per environment: infra/terraform/state/<env>.tfstate (spec §8).
TF_BACKEND = $(TF_DIR)/state/$(ENV).tfstate
# The stamp holds the backend path and a hash of the provider lock file and versions.tf from
# the last successful init. Init runs again only when the data directory is new, the backend
# path changed (a moved checkout: hence -reconfigure) or the providers changed (an upgrade,
# which is online anyway). Content hashes, not mtimes: a branch switch or a fresh clone
# touches mtimes and must not force an online init.
TF_STAMP = $(TF_DATA)/init.stamp
TF_INIT_KEY = echo "$(TF_BACKEND) $$(cat $(TF_DIR)/.terraform.lock.hcl $(TF_DIR)/versions.tf | sha256sum | cut -c1-64)"
TF_INIT = if [ "$$(cat $(TF_STAMP) 2>/dev/null)" != "$$($(TF_INIT_KEY))" ]; then \
		$(TF) init -input=false -reconfigure -backend-config=path=$(TF_BACKEND) \
		&& $(TF_INIT_KEY) > $(TF_STAMP); \
	else echo "terraform: $(TF_DATA) is initialised; skipping init"; fi
# ENV=local always carries the LocalStack endpoint; ENV=aws carries the account allow-list.
TF_VARS = -var env=$(ENV) \
	$(if $(filter local,$(ENV)),-var localstack_endpoint=$(LOCALSTACK_ENDPOINT)) \
	$(if $(filter aws,$(ENV)),-var aws_account_id=$(AWS_ACCOUNT_ID))

.PHONY: infra-env deploy destroy tf-check

infra-env:
	@if [ "$(origin ENV)" != "command line" ] || { [ "$(ENV)" != local ] && [ "$(ENV)" != aws ]; }; then \
		echo "error: this target needs ENV=local or ENV=aws on the command line" >&2; exit 2; fi

# ENV=aws: the saved plan that `make deploy` shows, confirms and then applies.
TF_PLAN = $(TF_DIR)/state/aws.tfplan

# ENV=local applies without asking. ENV=aws shows the plan and asks before applying it
# (applying a saved plan never prompts, so the confirmation is ours).
deploy: infra-env package
	mkdir -p $(REPO_ROOT)/infra/terraform/state $(REPO_ROOT)/state/$(ENV)
	@$(TF_INIT)
ifeq ($(ENV),aws)
	$(TF) plan -input=false -out=$(TF_PLAN) $(TF_VARS)
	@read -r -p "Apply this plan to account $(AWS_ACCOUNT_ID)? [y/N] " answer; \
		if [ "$$answer" != y ] && [ "$$answer" != Y ]; then \
			rm -f $(TF_PLAN); echo "deploy cancelled; nothing applied" >&2; exit 1; fi
	$(TF) apply -input=false $(TF_PLAN)
	rm -f $(TF_PLAN)
else
	$(TF) apply -input=false -auto-approve $(TF_VARS)
endif
	$(TF) output -json > $(REPO_ROOT)/state/$(ENV)/outputs.json.tmp
	mv $(REPO_ROOT)/state/$(ENV)/outputs.json.tmp $(REPO_ROOT)/state/$(ENV)/outputs.json

# ENV=aws: Terraform shows the destroy plan and asks for "yes".
destroy: infra-env package
	@$(TF_INIT)
	$(TF) destroy $(if $(filter local,$(ENV)),-input=false -auto-approve) $(TF_VARS)
	$(if $(filter aws,$(ENV)),rm -rf $(REPO_ROOT)/state/aws)

# Its own data directory without a backend, so it never touches an environment's init.
tf-check:
	terraform -chdir=$(TF_DIR) fmt -check -recursive
	TF_DATA_DIR=$(TF_DIR)/.terraform-check terraform -chdir=$(TF_DIR) init -input=false -backend=false
	TF_DATA_DIR=$(TF_DIR)/.terraform-check terraform -chdir=$(TF_DIR) validate

# ---- integration (spec §7.3) ----

.PHONY: integration

# The full chain on a fresh LocalStack. It starts with `make up`, so it wipes the current
# stack and backfill; run `make up` again before a demo backfill.
integration: up
	$(MAKE) deploy ENV=local
	LOCALSTACK=1 $(VENV)/bin/pytest -q -m integration tests/integration
