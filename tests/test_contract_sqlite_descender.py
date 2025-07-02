from pathlib import Path
import sqlite3
import sys

import pyarrow as pa
import pyarrow.parquet as pq

root_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root_dir / 'tool'))

import contract_sqlite_descender as desc


def _make_dataset(path: Path) -> None:
    table = pa.table({
        'block_number': [1, 2, 3],
        'address': ['0x1', '0x2', '0x3'],
        'bytecode': ['aa', 'bb', 'cc'],
    })
    pq.write_table(table, path)


def test_update_contract_db_reverse_basic(tmp_path):
    data = tmp_path / 'data.parquet'
    _make_dataset(data)
    db = tmp_path / 'out.db'
    desc.update_contract_db_reverse(str(db), size_limit_mb=1, parquet_path=str(data), page_rows=2)
    conn = sqlite3.connect(db)
    rows = [r[0] for r in conn.execute('SELECT block_number FROM contracts ORDER BY block_number DESC')]
    meta = {k: v for k, v in conn.execute('SELECT key, value FROM meta')}
    conn.close()
    assert rows == [3, 2, 1]
    assert meta['lowest_block'] == '0'


def test_update_contract_db_reverse_progress_cb(tmp_path):
    data = tmp_path / 'data.parquet'
    _make_dataset(data)
    db = tmp_path / 'out.db'
    calls: list[str] = []

    desc.update_contract_db_reverse(
        str(db),
        size_limit_mb=1,
        parquet_path=str(data),
        page_rows=2,
        progress_cb=calls.append,
    )
    assert calls


def test_cli_gui_invoked(tmp_path, monkeypatch):
    data = tmp_path / 'data.parquet'
    _make_dataset(data)
    db = tmp_path / 'out.db'

    called = {}

    def dummy_run(*args, **kwargs):
        called['run'] = args
        called['kwargs'] = kwargs

    class DummyGUI:
        def __init__(self, *args, **kwargs):
            called['gui_init'] = args
        def run(self):
            called['gui_run'] = True
        def _log(self, msg):
            called['log'] = msg

    monkeypatch.setattr(desc, 'run', dummy_run)
    monkeypatch.setattr(desc, 'SimpleGUI', DummyGUI)
    monkeypatch.setattr(
        sys,
        'argv',
        ['contract_sqlite_descender.py', '--db', str(db), '--size-limit', '1', '--gui'],
    )
    desc.main()
    assert called['gui_run']
    assert called['run'][0] == desc.DEFAULT_PARQUET_DATASET
    assert called['run'][4] == desc.DEFAULT_PAGE_ROWS
    assert called['kwargs']['progress_cb']

