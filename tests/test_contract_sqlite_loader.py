from pathlib import Path
import sqlite3

import pyarrow as pa
import pyarrow.parquet as pq

import sys
root_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root_dir / 'tool'))

import contract_sqlite_loader as loader


def _make_dataset(path: Path) -> None:
    table = pa.table({
        'block_number': [1, 2, 3],
        'address': ['0x1', '0x2', '0x3'],
        'bytecode': ['aa', 'bb', 'cc'],
    })
    pq.write_table(table, path)


def test_update_contract_db_basic(tmp_path):
    data = tmp_path / 'data.parquet'
    _make_dataset(data)
    db = tmp_path / 'out.db'
    loader.update_contract_db(str(data), str(db), start_block=2, end_block=3)
    conn = sqlite3.connect(db)
    rows = list(conn.execute('SELECT address, bytecode, block_number FROM contracts ORDER BY block_number'))
    conn.close()
    assert rows == [
        ('0x2', 'bb', 2),
        ('0x3', 'cc', 3),
    ]
    conn = sqlite3.connect(db)
    meta = {k: v for k, v in conn.execute('SELECT key, value FROM meta')}
    conn.close()
    assert meta['newest_block'] == '3'
    assert meta['oldest_block'] == '2'


def test_update_contract_db_default_latest(tmp_path):
    data = tmp_path / 'data.parquet'
    _make_dataset(data)
    db = tmp_path / 'out.db'
    loader.update_contract_db(str(data), str(db))
    conn = sqlite3.connect(db)
    rows = list(conn.execute('SELECT block_number FROM contracts'))
    meta = {k: v for k, v in conn.execute('SELECT key, value FROM meta')}
    conn.close()
    assert [r[0] for r in rows] == [3]
    assert meta['newest_block'] == '3'
    assert meta['oldest_block'] == '3'


def test_size_limit_enforced(tmp_path):
    data = tmp_path / 'data.parquet'
    table = pa.table({
        'block_number': [1, 2, 3, 4],
        'address': ['0x1', '0x2', '0x3', '0x4'],
        'bytecode': ['aa', 'bb', 'cc', 'dd'],
    })
    pq.write_table(table, data)

    db = tmp_path / 'out.db'
    # tiny limit so not all rows fit
    loader.update_contract_db(str(data), str(db), size_limit_mb=0.0001, start_block=1, end_block=4)
    conn = sqlite3.connect(db)
    row_count = conn.execute('SELECT COUNT(*) FROM contracts').fetchone()[0]
    conn.close()
    assert row_count < 4
