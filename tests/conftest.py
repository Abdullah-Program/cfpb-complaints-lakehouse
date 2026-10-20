import os

# Keep the Spark test session small and fast. Must be set before the session is created.
os.environ.setdefault("SPARK_DRIVER_MEMORY", "1g")
os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")

import pytest  # noqa: E402

from pipeline.spark_session import get_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = get_spark("cfpb-tests")
    yield s
    s.stop()


@pytest.fixture()
def lake(tmp_path, monkeypatch):
    """Isolated lake root per test."""
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "lake"))
    return tmp_path
