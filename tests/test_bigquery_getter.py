from pathlib import Path
import sys

import pytest

root_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root_dir / 'src'))

from data_getters import DataGetterBigQuery

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
