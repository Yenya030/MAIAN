import sqlite3
from pathlib import Path
import sys

root_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root_dir / 'tool'))

import contract_db_viewer as viewer


def _create_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        'CREATE TABLE contracts(address TEXT, bytecode TEXT, block_number INTEGER)'
    )
    conn.executemany(
        'INSERT INTO contracts VALUES(?, ?, ?)',
        [
            ('0x1', 'aa', 1),
            ('0x2', 'bb', 2),
            ('0x3', 'cc', 3),
        ],
    )
    conn.commit()
    conn.close()


def test_fetch_rows(tmp_path):
    db = tmp_path / 'out.db'
    _create_db(db)
    rows = viewer.fetch_rows(str(db), 2)
    assert rows == [
        ('0x1', 'aa', 1),
        ('0x2', 'bb', 2),
    ]


def test_format_rows():
    text = viewer.format_rows([
        ('0x1', 'aa', 1),
        ('0x2', 'bb', 2),
    ])
    assert '0x1' in text
    assert 'aa' in text
    assert '2' in text

