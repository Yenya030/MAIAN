import json
from pathlib import Path
from unittest import mock

import sys
root_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root_dir / 'tool'))
import contract_downloader


def test_prepend_and_limit(tmp_path):
    f = tmp_path / 'contracts.jsonl'
    m = tmp_path / 'meta.json'
    # set limit small so only one entry kept
    with mock.patch.object(contract_downloader, 'fetch_contracts', return_value=[{'address':'0x1','bytecode':'aa','block':1}]):
        with mock.patch.object(contract_downloader, 'get_latest_block', return_value=1):
            contract_downloader.update_contract_store('k', contract_file=str(f), metadata_file=str(m), size_limit_mb=0.00005, start_block=1, end_block=1)
    with mock.patch.object(contract_downloader, 'fetch_contracts', return_value=[{'address':'0x2','bytecode':'bb','block':2}]):
        contract_downloader.update_contract_store('k', contract_file=str(f), metadata_file=str(m), size_limit_mb=0.00005, start_block=2, end_block=2)
    lines = f.read_text().splitlines()
    assert json.loads(lines[0])['block'] == 2
    meta = json.loads(m.read_text())
    assert meta['newest_block'] == 2
    assert meta['oldest_block'] == 2


def test_get_latest_block(monkeypatch):
    def fake_get(url, params=None, timeout=10):
        class R:
            def raise_for_status(self):
                pass
            def json(self):
                return {'result': '0x10'}
        return R()
    monkeypatch.setattr(contract_downloader.requests, 'get', fake_get)
    block = contract_downloader.get_latest_block('k')
    assert block == 16

