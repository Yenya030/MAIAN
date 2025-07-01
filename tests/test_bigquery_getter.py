from pathlib import Path
import sys

import pytest

root_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root_dir / 'src'))

from data_getters import DataGetterBigQuery
from data_getters import bigquery_getter


PROXY_PREFIXES = (
    "0x363d3d373d3d3d363d73",
    "0x3660008037600080",
)


def _has_creds() -> bool:
    try:
        DataGetterBigQuery()
        return True
    except Exception:
        return False


@pytest.mark.skipif(not _has_creds(), reason="No GCP credentials")
def test_bigquery_basic():
    start = 17000000
    end = start + 20
    getter = DataGetterBigQuery(page_rows=1000)
    pages = list(getter.fetch_chunk(start, end))
    rows = [row for page in pages for row in page]
    assert rows
    assert getter.last_job.total_bytes_processed < 1_000_000_000
    for _, code in rows:
        assert not code.startswith(PROXY_PREFIXES)


def test_bigquery_uses_api_key(monkeypatch):
    captured = {}

    class DummyResult:
        def __init__(self):
            self.pages = [[]]

    class DummyJob:
        def result(self, page_size=None):
            return DummyResult()

    class DummyClient:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def query(self, sql, job_config=None):
            return DummyJob()

    monkeypatch.setattr(bigquery_getter, "bigquery", type(
        "DummyModule",
        (),
        {
            "Client": DummyClient,
            "QueryJobConfig": bigquery_getter.bigquery.QueryJobConfig,
            "ScalarQueryParameter": bigquery_getter.bigquery.ScalarQueryParameter,
        },
    ))

    g = DataGetterBigQuery(api_key="XYZ", project_id="proj", page_rows=1)
    list(g.fetch_chunk(1, 2))

    assert "client_options" in captured
    assert captured["client_options"].api_key == "XYZ"
    assert captured.get("project") == "proj"
