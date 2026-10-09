"""The $5 budget root and the guard that makes it come first (spec §8). Nothing reaches AWS:
`make -n` only prints, and budget-guard only reads a local state file."""

import json
import re

from tests.helpers import ROOT as REPO
from tests.helpers import make

ACCOUNT = "AWS_ACCOUNT_ID=123456789012"
OTHER_ACCOUNT = "AWS_ACCOUNT_ID=210987654321"
HINT = "make budget ENV=aws AWS_ACCOUNT_ID=<id> BUDGET_EMAIL=<address>"


def _budget_state(account_id: str) -> str:
    resource = {
        "type": "aws_budgets_budget",
        "name": "monthly",
        "instances": [{"attributes": {"account_id": account_id}}],
    }
    return json.dumps({"resources": [resource]}, indent=2)


def test_the_budget_is_5_usd_a_month_with_actual_and_forecast_alerts():
    main = (REPO / "infra" / "budget" / "main.tf").read_text()
    assert re.search(r'limit_amount\s*=\s*"5"', main)
    assert re.search(r'time_unit\s*=\s*"MONTHLY"', main)
    alerts = re.findall(
        r"threshold\s*=\s*(\d+)\s+threshold_type.*?notification_type\s*=\s*\"(\w+)\"", main, re.S
    )
    assert alerts == [("80", "ACTUAL"), ("100", "FORECASTED")]


def test_budget_guard_blocks_aws_until_the_budget_state_holds_a_budget_in_the_account(tmp_path):
    state = tmp_path / "aws.tfstate"
    guard = ["budget-guard", "ENV=aws", f"BUDGET_STATE={state}"]
    missing = make(*guard, ACCOUNT)
    assert missing.returncode == 2
    assert HINT in missing.stderr
    state.write_text(json.dumps({"resources": []}))  # destroyed: still blocked
    assert make(*guard, ACCOUNT).returncode == 2
    state.write_text(_budget_state("123456789012"))
    assert make(*guard, ACCOUNT).returncode == 0
    # A budget created for another account (or an earlier attempt) does not unlock this one.
    other = make(*guard, OTHER_ACCOUNT)
    assert other.returncode == 2
    assert "no budget for account 210987654321" in other.stderr
    assert HINT in other.stderr
    assert make("budget-guard", "ENV=local", f"BUDGET_STATE={tmp_path / 'none'}").returncode == 0


def test_deploy_aws_runs_the_budget_guard_before_terraform():
    out = make("-n", "deploy", "ENV=aws", ACCOUNT).stdout
    assert out.index("aws_budgets_budget") < out.index(" plan -input=false")


def test_make_budget_plans_asks_then_applies_into_its_own_state():
    out = make("-n", "budget", "ENV=aws", ACCOUNT, "BUDGET_EMAIL=me@example.com").stdout
    assert f"-backend-config=path={REPO}/infra/budget/state/aws.tfstate" in out
    assert "-var aws_account_id=123456789012 -var email=me@example.com" in out
    assert out.index(" plan -input=false") < out.index("[y/N]") < out.index(" apply -input=false")


def test_tf_check_validates_both_roots():
    out = make("-n", "tf-check").stdout
    assert f"for root in {REPO}/infra/terraform {REPO}/infra/budget" in out
