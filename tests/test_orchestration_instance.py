import shutil
from pathlib import Path

from dagster import DagsterInstance
from dagster._core.run_coordinator import QueuedRunCoordinator

YAML = Path(__file__).resolve().parents[1] / "orchestration" / "dagster.yaml"


def test_the_instance_queues_runs_and_the_duckdb_pool_admits_one_op(tmp_path):
    shutil.copy(YAML, tmp_path / "dagster.yaml")
    with DagsterInstance.from_config(str(tmp_path)) as instance:
        assert isinstance(instance.run_coordinator, QueuedRunCoordinator)
        pools = instance.get_concurrency_config().pool_config
        assert (pools.default_pool_limit, pools.pool_granularity.value) == (1, "op")
        assert instance.event_log_storage.get_concurrency_info("duckdb").limit == 1
        assert not instance.telemetry_enabled
