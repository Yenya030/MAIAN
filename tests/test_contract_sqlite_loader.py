from pathlib import Path
import sqlite3
import threading
import time

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


def test_run_continuous_updates_with_new_blocks(tmp_path):
    data = tmp_path / 'data.parquet'
    # initial dataset with a single block
    table = pa.table({
        'block_number': [1],
        'address': ['0x1'],
        'bytecode': ['aa'],
    })
    pq.write_table(table, data)

    db = tmp_path / 'out.db'

    def runner():
        loader.run_continuous(str(data), str(db), interval=0.2, max_rounds=2)

    t = threading.Thread(target=runner)
    t.start()

    # wait for the first block to be written
    for _ in range(20):
        if db.exists():
            try:
                conn = sqlite3.connect(db)
                count = conn.execute('SELECT COUNT(*) FROM contracts').fetchone()[0]
                conn.close()
            except sqlite3.OperationalError:
                count = 0
            if count:
                break
        time.sleep(0.05)

    # extend dataset with a new block before the next iteration
    table = pa.table({
        'block_number': [1, 2],
        'address': ['0x1', '0x2'],
        'bytecode': ['aa', 'bb'],
    })
    pq.write_table(table, data)

    t.join()

    conn = sqlite3.connect(db)
    blocks = [r[0] for r in conn.execute('SELECT block_number FROM contracts ORDER BY block_number')]
    conn.close()
    assert blocks == [1, 2]
