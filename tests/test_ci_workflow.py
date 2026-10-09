"""The CI workflow's shape (actionlint is not part of the toolchain; this is the local check)."""

import yaml

from tests.helpers import ROOT


def workflow() -> dict:
    data = yaml.safe_load((ROOT / ".github" / "workflows" / "ci.yml").read_text())
    data["on"] = data.pop(True, data.get("on"))  # YAML 1.1 reads the key `on` as true
    return data


def test_every_job_is_read_only_and_time_boxed():
    wf = workflow()
    assert wf["permissions"] == {"contents": "read"}
    allowed = {"if", "runs-on", "timeout-minutes", "permissions", "steps", "needs", "concurrency"}
    for name, job in wf["jobs"].items():
        assert job["runs-on"] == "ubuntu-24.04", name
        assert isinstance(job["timeout-minutes"], int), name
        assert set(job) <= allowed, name


def test_the_localstack_job_runs_integration_on_main_pushes_and_on_demand():
    wf = workflow()
    assert "workflow_dispatch" in wf["on"]
    job = wf["jobs"]["localstack"]
    assert job["if"] == (
        "(github.event_name == 'push' && github.ref == 'refs/heads/main') "
        "|| github.event_name == 'workflow_dispatch'"
    )
    assert job["timeout-minutes"] == 30
    assert job["permissions"] == {"contents": "read"}
    # A lint/unit failure must not spend runner minutes on LocalStack, and a newer push
    # to the same ref supersedes a running one.
    assert job["needs"] == "offline"
    assert "offline" in wf["jobs"]
    assert job["concurrency"] == {
        "group": "localstack-${{ github.ref }}",
        "cancel-in-progress": True,
    }
    runs = [step["run"] for step in job["steps"] if "run" in step]
    assert runs[:2] == ["make venv", "make integration"]
    terraform = next(s for s in job["steps"] if s.get("uses", "").startswith("hashicorp/"))
    assert terraform["with"]["terraform_wrapper"] is False
    assert "${{ secrets." not in (ROOT / ".github" / "workflows" / "ci.yml").read_text()
