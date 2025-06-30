import sys
from pathlib import Path
import types

root_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root_dir / 'tool'))

import contract_stats


class DummyReceipt:
    def __init__(self, contract_address=None):
        self.contractAddress = contract_address


class DummyTx:
    def __init__(self, to=None, tx_hash='0x0'):
        self.to = to
        self.hash = tx_hash


class DummyBlock:
    def __init__(self, txs):
        self.transactions = txs


class DummyEth:
    def __init__(self):
        self.block_number = 0
        self._blocks = []
        self._codes = {}
        self._balances = {}
        self._receipts = {}

    def get_block(self, num, full_transactions=False):
        return self._blocks[num]

    def get_code(self, addr):
        return self._codes.get(addr, b'')

    def get_balance(self, addr):
        return self._balances.get(addr, 0)

    def get_transaction_receipt(self, tx_hash):
        return self._receipts.get(tx_hash, DummyReceipt())


class DummyWeb3:
    def __init__(self):
        self.eth = DummyEth()

    def is_connected(self):
        return True


def test_collect_contract_addresses():
    w3 = DummyWeb3()
    w3.eth.block_number = 1

    tx1 = DummyTx(to='0x1', tx_hash='h1')
    tx2 = DummyTx(to=None, tx_hash='h2')
    block0 = DummyBlock([tx1, tx2])
    block1 = DummyBlock([DummyTx(to='0x1', tx_hash='h3')])
    w3.eth._blocks = [block0, block1]

    w3.eth._codes['0x1'] = b'abc'
    w3.eth._codes['0x2'] = b'def'
    w3.eth._balances['0x1'] = 100
    w3.eth._balances['0x2'] = 50
    w3.eth._receipts['h2'] = DummyReceipt('0x2')

    addrs = contract_stats.collect_contract_addresses(w3, 0, 1)
    assert addrs == {'0x1', '0x2'}


def test_count_contracts(monkeypatch):
    w3 = DummyWeb3()
    w3.eth.block_number = 0
    tx = DummyTx(to='0x1', tx_hash='h1')
    w3.eth._blocks = [DummyBlock([tx])]
    w3.eth._codes['0x1'] = b'abc'
    w3.eth._balances['0x1'] = 200

    def fake_provider_url(network):
        return 'http://example.com'

    class DummyWeb3Class:
        HTTPProvider = staticmethod(lambda url: None)

        def __new__(cls, provider):
            return w3

    monkeypatch.setattr(contract_stats, 'get_provider_url', fake_provider_url)
    monkeypatch.setattr(contract_stats, 'Web3', DummyWeb3Class)

    stats = contract_stats.count_contracts('mainnet', 0, 0)
    assert stats == {
        'network': 'mainnet',
        'start_block': 0,
        'end_block': 0,
        'contract_count': 1,
        'total_balance_wei': 200,
        'total_code_size': 3,
    }


def test_file_data_store(tmp_path):
    store_path = tmp_path / 'out.csv'
    store = contract_stats.FileDataStore(str(store_path))
    stats = {
        'network': 'mainnet',
        'start_block': 0,
        'end_block': 0,
        'contract_count': 2,
        'total_balance_wei': 1000,
        'total_code_size': 10,
    }
    store.save(stats)

    content = store_path.read_text().splitlines()
    assert content[0].startswith('network,start_block')
    assert content[1].split(',') == [
        'mainnet', '0', '0', '2', '1000', '10'
    ]
