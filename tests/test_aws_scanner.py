from pathlib import Path
import json
import types

import pyarrow as pa
import pyarrow.parquet as pq

import sys
root_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root_dir / 'tool'))

import aws_scanner


def _make_dataset(path: Path, blocks: list[int]) -> None:
    table = pa.table({
        'block_number': blocks,
        'address': [f'0x{b}' for b in blocks],
        'bytecode': [f'{b:02x}' for b in blocks],
    })
    pq.write_table(table, path)


def test_scan_once_creates_state_and_report(tmp_path, monkeypatch):
    data = tmp_path / 'data.parquet'
    _make_dataset(data, [1, 2, 3])
    state = tmp_path / 'state.json'
    report = tmp_path / 'report.jsonl'

    monkeypatch.setattr(aws_scanner, 'run_checks', lambda b, a: {
        'suicidal': True,
        'prodigal': False,
        'greedy': False,
    })
    msgs: list[str] = []
    aws_scanner.scan_once(
        str(data),
        state_file=str(state),
        report_file=str(report),
        batch_blocks=1,
        page_rows=1,
        progress_cb=msgs.append,
    )
    st = json.loads(state.read_text())
    assert st['next_block'] == 2
    lines = report.read_text().splitlines()
    assert len(lines) == 1
    entry = json.loads(lines[0])
    assert entry['address'] == '0x3'
    assert entry['suicidal'] is True
    assert msgs


def test_scan_once_resumes_and_handles_new_blocks(tmp_path, monkeypatch):
    data = tmp_path / 'data.parquet'
    _make_dataset(data, [1, 2, 3])
    state = tmp_path / 'state.json'
    report = tmp_path / 'report.jsonl'
    monkeypatch.setattr(aws_scanner, 'run_checks', lambda b, a: {
        'suicidal': False,
        'prodigal': False,
        'greedy': False,
    })
    aws_scanner.scan_once(str(data), state_file=str(state), report_file=str(report), batch_blocks=1)
    # extend dataset with a new block
    _make_dataset(data, [1, 2, 3, 4])
    aws_scanner.scan_once(str(data), state_file=str(state), report_file=str(report), batch_blocks=1)
    st = json.loads(state.read_text())
    assert st['next_block'] == 3
    lines = report.read_text().splitlines()
    assert len(lines) == 2

