"""Shared fixtures. Spark tests use Databricks Connect or a local JVM, and skip if neither is available."""

import pytest


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.serverless().getOrCreate()
    except Exception:
        pass
    try:
        from pyspark.sql import SparkSession

        return SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").getOrCreate()
    except Exception as exc:
        pytest.skip(f"No Spark available: {exc}")


class FakeClient:
    """Serves canned responses by URL and records what was requested."""

    def __init__(self, responses: dict[str, bytes]):
        self.responses = responses
        self.requests: list[str] = []

    def get(self, url: str) -> bytes:
        self.requests.append(url)
        return self.responses[url]


@pytest.fixture
def fake_client():
    return FakeClient
