"""M7: the DynamoDB tables carry the onside- prefix, the same in Terraform and in moto."""

import re
from pathlib import Path

from tests.aws_fixtures import EVALUATION_TABLE, PERSON_TABLE, PROPERTY_TABLE

ROOT = Path(__file__).resolve().parents[1]
TABLE = re.compile(r'resource "aws_dynamodb_table" "(\w+)" \{\s*name\s*=\s*"([^"]+)"')


def terraform_tables() -> dict[str, str]:
    return dict(TABLE.findall((ROOT / "infra" / "terraform" / "storage.tf").read_text()))


def test_terraform_tables_are_prefixed():
    assert terraform_tables() == {
        "person_state": "onside-person-state",
        "evaluations": "onside-evaluations",
        "properties": "onside-properties",
    }


def test_moto_stack_uses_the_terraform_names(aws):
    assert {PERSON_TABLE, EVALUATION_TABLE, PROPERTY_TABLE} == set(terraform_tables().values())
    assert set(aws.dynamodb.list_tables()["TableNames"]) == set(terraform_tables().values())
    outputs = {k: v["value"] for k, v in aws.outputs().items()}
    assert (outputs["person_table"], outputs["evaluation_table"], outputs["property_table"]) == (
        PERSON_TABLE,
        EVALUATION_TABLE,
        PROPERTY_TABLE,
    )
