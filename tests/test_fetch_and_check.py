import types
from unittest import mock

import sys
from pathlib import Path
import pytest

root_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root_dir / 'tool'))
sys.path.insert(0, str(root_dir))
import fetch_and_check

class DummyTx:
    def __init__(self, to):
        self.to = to

class DummyBlock:
    def __init__(self, txs):
        self.transactions = txs

class DummyWeb3:
    def __init__(self):
        self.eth = types.SimpleNamespace()
        self.eth.block_number = 1
        self.eth.get_block = self.get_block
        self.eth.get_code = self.get_code
        self.blocks = []
        self.codes = {}

    def is_connected(self):
        return True

    def eth_get_code(self, address):
        return self.codes.get(address, b"")

    def get_block(self, num, full_transactions=False):
        return self.blocks.pop(0)

    def get_code(self, address):
        return self.eth_get_code(address)

def test_random_contract_address():
    w3 = DummyWeb3()
    addr = '0xdeadbeef'
    w3.codes[addr] = b'abc'
    w3.blocks.append(DummyBlock([DummyTx(addr)]))
    result = fetch_and_check.random_contract_address(w3, search_depth=1, attempts=1)
    assert result == addr

def test_run_checks_calls():
    bytecode = '00'
    addr = '0x0'
    with (
        mock.patch('fetch_and_check.check_one_contract_on_suicide', return_value=False) as ms,
        mock.patch('fetch_and_check.check_one_contract_on_ether_leak', return_value=False) as ml,
        mock.patch('fetch_and_check.check_one_contract_on_ether_lock', return_value=False) as mk,
    ):
        res = fetch_and_check.run_checks(bytecode, addr)
        assert set(res.keys()) == {
            'suicidal', 'suicide_time', 'prodigal', 'prodigal_time',
            'greedy', 'greedy_time'
        }
        ms.assert_called_once()
        ml.assert_called_once()
        mk.assert_called_once()


def test_get_provider_url_mainnet():
    url = fetch_and_check.get_provider_url('mainnet')
    assert url == fetch_and_check.NETWORK_PROVIDERS['mainnet']


def test_get_provider_url_invalid():
    with pytest.raises(ValueError):
        fetch_and_check.get_provider_url('foobar')


def test_scan_multiple_contracts():
    with mock.patch('fetch_and_check.scan_random_contract', return_value={'a':1}) as m:
        res = fetch_and_check.scan_multiple_contracts(count=3, network='mainnet')
        assert res == [{'a':1}, {'a':1}, {'a':1}]
        assert m.call_count == 3
