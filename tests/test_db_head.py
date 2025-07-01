from pathlib import Path
import sqlite3
import sys

root_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root_dir / 'tool'))

import db_head


def _make_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE contracts (
            address TEXT PRIMARY KEY,
            bytecode TEXT NOT NULL,
            block_number INTEGER NOT NULL
        )
        """
    )
    rows = [
        ('0x1', 'aa', 1),
        ('0x2', 'bb', 2),
        ('0x3', 'cc', 3),
    ]
    conn.executemany(
        'INSERT INTO contracts(address, bytecode, block_number) VALUES(?, ?, ?)',
        rows,
    )
    conn.commit()
    conn.close()


def test_get_entries(tmp_path):
    db = tmp_path / 'c.db'
    _make_db(db)
    rows = db_head.get_entries(str(db), limit=2)
    assert rows == [
        ('0x1', 'aa', 1),
        ('0x2', 'bb', 2),
    ]


def test_cli(monkeypatch, tmp_path, capsys):
    db = tmp_path / 'c.db'
    _make_db(db)
    monkeypatch.setattr(sys, 'argv', ['db_head.py', str(db), '--count', '1'])
    db_head.main()
    out = capsys.readouterr().out
    assert '0x1' in out
