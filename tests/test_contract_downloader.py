import json
from pathlib import Path
from unittest import mock

import sys
root_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root_dir / 'tool'))
import contract_downloader


class DummySource(contract_downloader.DataSource):
    def __init__(self, responses):
        self.responses = responses
        self.latest = 1

    def latest_block(self):
        return self.latest

    def fetch(self, start_block, end_block):
        return self.responses.pop(0)


def test_prepend_and_limit(tmp_path):
    f = tmp_path / 'contracts.jsonl'
    m = tmp_path / 'meta.json'
    source = DummySource([[{'address': '0x1', 'bytecode': 'aa', 'block': 1}],
                          [{'address': '0x2', 'bytecode': 'bb', 'block': 2}]])
    # set limit small so only one entry kept
    contract_downloader.update_contract_store(
        source,
        contract_file=str(f),
        metadata_file=str(m),
        size_limit_mb=0.00005,
        start_block=1,
        end_block=1,
    )
    source.latest = 2
    contract_downloader.update_contract_store(
        source,
        contract_file=str(f),
        metadata_file=str(m),
        size_limit_mb=0.00005,
        start_block=2,
        end_block=2,
    )
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


def test_etherscan_verified(monkeypatch):
    captured = {}

    def fake_get(url, params=None, timeout=10):
        captured['params'] = params
        class R:
            def raise_for_status(self):
                pass

            def json(self):
                return {'status': '1', 'result': []}

        return R()

    monkeypatch.setattr(contract_downloader.requests, 'get', fake_get)
    source = contract_downloader.EtherscanSource('k', verified_only=True)
    source.fetch(1, 2)
    assert captured['params'].get('filter') == 'verified'


def test_etherscan_latest_block(monkeypatch):
    """Ensure a request is made and the block number parsed."""
    captured = {}

    def fake_get(url, params=None, timeout=10):
        captured['params'] = params
        class R:
            def raise_for_status(self):
                pass

            def json(self):
                return {'result': '0x20'}

        return R()

    monkeypatch.setattr(contract_downloader.requests, 'get', fake_get)
    source = contract_downloader.EtherscanSource('k')
    block = source.latest_block()
    assert block == 32
    assert captured['params']['action'] == 'eth_blockNumber'


def test_etherscan_fetch_parsing(monkeypatch):
    """Verify contracts are parsed from the API response."""
    captured = {}
    data = {
        'status': '1',
        'result': [
            {
                'ContractAddress': '0x1',
                'Bytecode': '0xaa',
                'BlockNumber': '10',
            }
        ],
    }

    def fake_get(url, params=None, timeout=10):
        captured['params'] = params
        class R:
            def raise_for_status(self):
                pass

            def json(self):
                return data

        return R()

    monkeypatch.setattr(contract_downloader.requests, 'get', fake_get)
    source = contract_downloader.EtherscanSource('k')
    res = source.fetch(10, 10)
    assert res == [{'address': '0x1', 'bytecode': '0xaa', 'block': 10}]
    assert captured['params']['startblock'] == 10
    assert captured['params']['endblock'] == 10


class DummyTx:
    def __init__(self, to=None, tx_hash='0x0'):
        self.to = to
        self.hash = tx_hash


class DummyReceipt:
    def __init__(self, contract_address=None):
        self.contractAddress = contract_address


class DummyBlock:
    def __init__(self, txs):
        self.transactions = txs


class DummyEth:
    def __init__(self):
        self.block_number = 1
        self._blocks = []
        self._codes = {}
        self._receipts = {}

    def get_block(self, num, full_transactions=False):
        return self._blocks[num]

    def get_code(self, addr):
        return self._codes.get(addr, b"")

    def get_transaction_receipt(self, tx_hash):
        return self._receipts.get(tx_hash, DummyReceipt())


class DummyWeb3:
    def __init__(self):
        self.eth = DummyEth()

    def is_connected(self):
        return True


def test_rpc_source_fetch(monkeypatch):
    w3 = DummyWeb3()
    tx1 = DummyTx(to='0x1')
    tx2 = DummyTx(to=None, tx_hash='h2')
    block = DummyBlock([tx1, tx2])
    w3.eth._blocks = [block]
    w3.eth._codes['0x1'] = b'aa'
    w3.eth._codes['0x2'] = b'bb'
    w3.eth._receipts['h2'] = DummyReceipt('0x2')

    class DummyWeb3Class:
        HTTPProvider = staticmethod(lambda url: None)

        def __new__(cls, provider):
            return w3

    monkeypatch.setattr(contract_downloader, 'Web3', DummyWeb3Class)
    source = contract_downloader.RPCSource('url')
    res = source.fetch(0, 0)
    assert {c['address'] for c in res} == {'0x1', '0x2'}


class RangeSource(contract_downloader.DataSource):
    """Dummy source that returns one contract per block."""

    def __init__(self, latest: int) -> None:
        self.latest = latest
        self.calls = []

    def latest_block(self) -> int:
        return self.latest

    def fetch(self, start_block: int, end_block: int):
        self.calls.append((start_block, end_block))
        return [
            {"address": hex(i), "bytecode": "aa", "block": i}
            for i in range(start_block, end_block + 1)
        ]


def _run_two_stage_download(tmp_path, source):
    cfile = tmp_path / "out.jsonl"
    mfile = tmp_path / "meta.json"
    start1 = source.latest_block() - 100
    end1 = source.latest_block()
    contract_downloader.update_contract_store(
        source,
        contract_file=str(cfile),
        metadata_file=str(mfile),
        start_block=start1,
        end_block=end1,
    )
    start2 = source.latest_block() - 1000
    end2 = start1 - 1
    contract_downloader.update_contract_store(
        source,
        contract_file=str(cfile),
        metadata_file=str(mfile),
        start_block=start2,
        end_block=end2,
    )
    return cfile, mfile, start1, end1, start2, end2, source.calls


def test_update_contract_store_two_stage(tmp_path):
    source = RangeSource(2000)
    cfile, mfile, start1, end1, start2, end2, calls = _run_two_stage_download(
        tmp_path, source
    )
    lines = cfile.read_text().splitlines()
    assert len(lines) == 1001
    meta = json.loads(mfile.read_text())
    assert meta["newest_block"] == end1
    assert meta["oldest_block"] == start2
    assert calls == [(start1, end1), (start2, end2)]


def test_update_contract_store_two_stage_rpc(tmp_path):
    source = RangeSource(3000)
    cfile, mfile, start1, end1, start2, end2, calls = _run_two_stage_download(
        tmp_path, source
    )
    lines = cfile.read_text().splitlines()
    assert len(lines) == 1001
    meta = json.loads(mfile.read_text())
    assert meta["newest_block"] == end1
    assert meta["oldest_block"] == start2
    assert calls == [(start1, end1), (start2, end2)]


def test_update_contract_store_default_latest(monkeypatch, tmp_path):
    """When no range is given and no metadata exists, use the latest block."""
    source = RangeSource(500)
    cfile = tmp_path / "c.jsonl"
    mfile = tmp_path / "m.json"
    contract_downloader.update_contract_store(
        source,
        contract_file=str(cfile),
        metadata_file=str(mfile),
    )
    assert source.calls == [(500, 500)]
    lines = cfile.read_text().splitlines()
    assert len(lines) == 1
    meta = json.loads(mfile.read_text())
    assert meta["newest_block"] == 500
    assert meta["oldest_block"] == 500

