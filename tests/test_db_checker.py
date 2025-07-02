from pathlib import Path
import json
import sqlite3
import sys

root_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root_dir / 'tool'))

import db_checker


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
    ]
    conn.executemany(
        'INSERT INTO contracts(address, bytecode, block_number) VALUES(?, ?, ?)',
        rows,
    )
    conn.commit()
    conn.close()


def test_scan_database(monkeypatch, tmp_path):
    db = tmp_path / 'c.db'
    _make_db(db)

    results = [
        {'suicidal': True, 'prodigal': False, 'greedy': False},
        {'suicidal': False, 'prodigal': True, 'greedy': True},
    ]

    def fake_run(bytecode: str, address: str):
        return results.pop(0)

    monkeypatch.setattr(db_checker, 'run_checks', fake_run)

    msgs: list[str] = []
    summary = db_checker.scan_database(str(db), progress_cb=msgs.append)
    assert summary == {
        'total': 2,
        'scanned': 2,
        'suicidal': 1,
        'prodigal': 1,
        'greedy': 1,
    }
    assert msgs


def test_cli_writes_report(monkeypatch, tmp_path):
    db = tmp_path / 'c.db'
    report = tmp_path / 'out.json'
    _make_db(db)

    monkeypatch.setattr(
        db_checker,
        'run_checks',
        lambda b, a: {'suicidal': False, 'prodigal': False, 'greedy': False},
    )

    monkeypatch.setattr(
        sys,
        'argv',
        ['db_checker.py', '--db', str(db), '--report', str(report)]
    )
    db_checker.main()
    data = json.loads(report.read_text())
    assert data['scanned'] == 2

