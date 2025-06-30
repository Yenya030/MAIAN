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


def test_scan_multiple_contracts_unique(tmp_path):
    responses = [
        {'address': '0x1'},
        {'address': '0x1'},  # duplicate should be skipped
        {'address': '0x2'},
        {'address': '0x3'},
    ]

    def side_effect(*_, **__):
        return responses.pop(0)

    address_file = tmp_path / 'addrs.txt'
    with mock.patch('fetch_and_check.scan_random_contract', side_effect=side_effect) as m:
        res = fetch_and_check.scan_multiple_contracts(
            count=3, network='mainnet', address_file=str(address_file)
        )
        assert [r['address'] for r in res] == ['0x1', '0x2', '0x3']
        assert m.call_count == 4
    assert address_file.read_text().splitlines() == ['0x1', '0x2', '0x3']

def test_scan_multiple_contracts_allow_duplicates():
    with mock.patch('fetch_and_check.scan_random_contract', return_value={'address': '0x1'}) as m:
        res = fetch_and_check.scan_multiple_contracts(count=2, network='mainnet', unique=False)
        assert res == [{'address': '0x1'}, {'address': '0x1'}]
        assert m.call_count == 2


def test_scan_multiple_contracts_respects_existing_file(tmp_path):
    address_file = tmp_path / 'addrs.txt'
    address_file.write_text('0x1\n')

    responses = [
        {'address': '0x1'},
        {'address': '0x2'},
        {'address': '0x3'},
    ]

    def side_effect(*_, **__):
        return responses.pop(0)

    with mock.patch('fetch_and_check.scan_random_contract', side_effect=side_effect) as m:
        res = fetch_and_check.scan_multiple_contracts(
            count=2,
            network='mainnet',
            address_file=str(address_file),
        )
        assert [r['address'] for r in res] == ['0x2', '0x3']
        assert m.call_count == 3
    assert address_file.read_text().splitlines() == ['0x1', '0x2', '0x3']


def test_vulnerable_addresses_written(tmp_path):
    report_dir = tmp_path / 'reports'

    reports = [
        {'address': '0x1', 'suicidal': True, 'prodigal': False, 'greedy': False},
        {'address': '0x2', 'suicidal': False, 'prodigal': True, 'greedy': True},
    ]

    def side_effect(*_, **__):
        return reports.pop(0)

    with mock.patch('fetch_and_check.scan_random_contract', side_effect=side_effect):
        fetch_and_check.scan_multiple_contracts(
            count=2,
            network='mainnet',
            address_file=None,
            report_dir=str(report_dir),
        )

    assert (report_dir / 'suicidal.txt').read_text().splitlines() == ['0x1']
    assert (report_dir / 'prodigal.txt').read_text().splitlines() == ['0x2']
    assert (report_dir / 'greedy.txt').read_text().splitlines() == ['0x2']

