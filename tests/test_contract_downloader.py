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

