from pathlib import Path
import sys
import json

root_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root_dir / 'tool'))

import java_bigquery_scanner


def test_scan_bigquery_with_java(monkeypatch, tmp_path):
    out_file = tmp_path / 'contracts.jsonl'

    def fake_fetcher(jar, dataset, start, end, output):
        data = [
            {"Address": "0x1", "ByteCode": "aa", "BlockNumber": 1},
            {"Address": "0x2", "ByteCode": "bb", "BlockNumber": 2},
        ]
        with open(output, 'w', encoding='utf-8') as fh:
            for row in data:
                fh.write(json.dumps(row) + '\n')

    monkeypatch.setattr(java_bigquery_scanner, 'run_java_fetcher', fake_fetcher)

    results = iter([
        {"suicidal": False, "prodigal": False, "greedy": False},
        {"suicidal": True, "prodigal": False, "greedy": True},
    ])
    monkeypatch.setattr(java_bigquery_scanner, 'run_checks', lambda b, a: next(results))

    reports = java_bigquery_scanner.scan_bigquery_with_java(
        'dataset.table',
        1,
        2,
        jar_path='dummy',
        output_file=str(out_file),
    )
    assert len(reports) == 2
    assert reports[1]['suicidal']
