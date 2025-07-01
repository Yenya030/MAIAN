import json
from pathlib import Path
from unittest import mock
import types
import pytest
import requests

root_dir = Path(__file__).resolve().parents[1]
import sys
sys.path.insert(0, str(root_dir / 'tool'))
import contract_downloader


def _make_response(data, url='http://example', status=200):
    class R:
        def __init__(self):
            self.status_code = status
            self.url = url
            self.content = json.dumps(data).encode('utf-8')

        def raise_for_status(self):
            if self.status_code >= 400:
                raise requests.HTTPError(self.status_code)

        def json(self):
            return data
    return R()

def test_successful_fetch_and_parse(monkeypatch, tmp_path):
    def fake_get(url, params=None, timeout=10):
        if params['action'] == 'eth_blockNumber':
            return _make_response({'result': '0x64'}, url=url)
        return _make_response({
            'status': '1',
            'result': [
                {'ContractAddress': '0x1', 'Bytecode': '0xaa', 'BlockNumber': '100'}
            ]
        }, url=url)

    monkeypatch.setattr(contract_downloader.requests, 'get', fake_get)
    source = contract_downloader.EtherscanSource('k')
    cfile = tmp_path / 'c.jsonl'
    mfile = tmp_path / 'm.json'
    contract_downloader.update_contract_store(
        source,
        contract_file=str(cfile),
        metadata_file=str(mfile),
    )
    lines = cfile.read_text().splitlines()
    assert len(lines) == 1
    obj = json.loads(lines[0])
    assert obj['address'] == '0x1'
    assert obj['bytecode'] == '0xaa'
    meta = json.loads(mfile.read_text())
    assert meta['newest_block'] == 100
    assert meta['oldest_block'] == 100

def test_http_failure(monkeypatch):
    def fake_get(*a, **k):
        raise requests.Timeout
    monkeypatch.setattr(contract_downloader.requests, 'get', fake_get)
    source = contract_downloader.EtherscanSource('k')
    with pytest.raises(requests.Timeout):
        source.latest_block()

def test_malformed_response(monkeypatch, tmp_path):
    def fake_get(url, params=None, timeout=10):
        if params['action'] == 'eth_blockNumber':
            return _make_response({'result': '0x64'}, url=url)
        # Missing Bytecode field
        return _make_response({'status': '1', 'result': [{'ContractAddress': '0x1'}]}, url=url)
    monkeypatch.setattr(contract_downloader.requests, 'get', fake_get)
    source = contract_downloader.EtherscanSource('k')
    cfile = tmp_path / 'c.jsonl'
    mfile = tmp_path / 'm.json'
    with pytest.raises(ValueError):
        contract_downloader.update_contract_store(
            source,
            contract_file=str(cfile),
            metadata_file=str(mfile),
        )


