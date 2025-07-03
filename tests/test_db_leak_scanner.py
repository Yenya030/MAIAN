import sqlite3
import sys
from pathlib import Path

root_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root_dir / 'tool'))

import db_leak_scanner


def _make_src_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE contracts (
            address TEXT PRIMARY KEY,
            bytecode TEXT NOT NULL,
            block_number INTEGER NOT NULL,
            checked INTEGER
        )
        """
    )
    rows = [
        ('0x1', 'aa', 1, 0),
        ('0x2', 'bb', 2, 1),  # already checked
        ('0x3', 'cc', 3, 0),
    ]
    conn.executemany(
        'INSERT INTO contracts(address, bytecode, block_number, checked) VALUES(?, ?, ?, ?)',
        rows,
    )
    conn.commit()
    conn.close()


def test_scan_for_leaks(monkeypatch, tmp_path):
    src = tmp_path / 'src.db'
    dst = tmp_path / 'dst.db'
    _make_src_db(src)

    results = [
        {'prodigal': True},
        {'prodigal': False},
    ]

    def fake_run(bytecode: str, address: str):
        return results.pop(0)

    monkeypatch.setattr(db_leak_scanner, 'run_checks', fake_run)

    summary = db_leak_scanner.scan_for_leaks(str(src), str(dst), progress_cb=lambda _: None)
    assert summary['scanned'] == 2
    assert summary['vulnerable'] == 1

    conn = sqlite3.connect(src)
    checked = dict(conn.execute('SELECT address, checked FROM contracts'))
    conn.close()
    assert checked == {'0x1': 1, '0x2': 1, '0x3': 1}

    conn = sqlite3.connect(dst)
    rows = list(conn.execute('SELECT address, block_number FROM contracts'))
    conn.close()
    assert rows == [('0x1', 1)]


def test_cli_runs(monkeypatch, tmp_path):
    src = tmp_path / 'src.db'
    dst = tmp_path / 'dst.db'
    _make_src_db(src)

    monkeypatch.setattr(db_leak_scanner, 'run_checks', lambda b, a: {'prodigal': False})
    monkeypatch.setattr(sys, 'argv', ['db_leak_scanner.py', '--src-db', str(src), '--dst-db', str(dst)])
    db_leak_scanner.main()

    conn = sqlite3.connect(src)
    checked = [r[0] for r in conn.execute('SELECT checked FROM contracts ORDER BY address')]
    conn.close()
    assert checked == [1, 1, 1]
