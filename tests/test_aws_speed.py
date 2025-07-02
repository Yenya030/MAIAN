from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

import sys
root_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root_dir / 'tool'))

import aws_speed


def _make_dataset(path: Path) -> None:
    table = pa.table({
        'block_number': [1, 2, 3],
        'address': ['0x1', '0x2', '0x3'],
        'bytecode': ['aa', 'bb', 'cc'],
    })
    pq.write_table(table, path)


def test_measure_speed_basic(tmp_path, monkeypatch):
    data = tmp_path / 'data.parquet'
    _make_dataset(data)
    times = iter([0.0, 2.0])
    monkeypatch.setattr(aws_speed.time, 'time', lambda: next(times))
    stats = aws_speed.measure_speed(str(data), 1, 3, page_rows=2)
    assert stats['contracts'] == 3
    assert stats['bytes'] == 3
    assert stats['seconds'] == 2.0
    assert stats['mb_per_second'] > 0


def test_main_prints_stats(tmp_path, monkeypatch, capsys):
    data = tmp_path / 'data.parquet'
    _make_dataset(data)
    monkeypatch.setattr(aws_speed, '_latest_block', lambda p: 3)
    times = iter([0.0, 1.0])
    monkeypatch.setattr(aws_speed.time, 'time', lambda: next(times))
    monkeypatch.setattr(sys, 'argv', ['aws_speed.py', str(data), '--blocks', '3'])
    aws_speed.main()
    out = capsys.readouterr().out
    assert 'MB/s' in out

