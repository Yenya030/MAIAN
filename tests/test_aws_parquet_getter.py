from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

import sys
root_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root_dir / 'tool'))

from data_getters import DataGetterAWSParquet


def _make_sample(path: Path) -> None:
    table = pa.table({
        'block_number': [1, 2, 3, 4],
        'address': ['0x1', '0x2', '0x3', '0x4'],
        'bytecode': ['aa', 'bb', 'cc', 'dd'],
    })
    pq.write_table(table, path)


def test_parquet_getter_basic(tmp_path):
    f = tmp_path / 'data.parquet'
    _make_sample(f)
    g = DataGetterAWSParquet(str(f), page_rows=2)
    pages = list(g.fetch_chunk(2, 3))
    rows = [r for page in pages for r in page]
    assert rows == [('0x2', 'bb', 2), ('0x3', 'cc', 3)]
