"""Every `make` target that the README and the AWS runbook show exists in the Makefile."""

import re

import pytest

from tests.helpers import ROOT

TARGETS = set(re.findall(r"^([a-z][a-z0-9-]*):", (ROOT / "Makefile").read_text(), re.M))


@pytest.mark.parametrize("doc", ["README.md", "docs/aws-deploy.md"])
def test_every_make_target_in_the_doc_exists(doc):
    shown = set(re.findall(r"\bmake ([a-z][a-z0-9-]*)", (ROOT / doc).read_text()))
    assert shown, doc
    assert shown <= TARGETS, sorted(shown - TARGETS)
