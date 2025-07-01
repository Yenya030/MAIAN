import types
import sys
from pathlib import Path
import pytest

root_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root_dir / 'tool'))
import fetch_and_check


class DummyWeb3:
    def __init__(self):
        self.eth = types.SimpleNamespace()
        self.eth.get_code = self.get_code
        self.codes = {}

    def get_code(self, address):
        return self.codes.get(address, b'')


def test_fetch_contract_bytecode_returns_hex():
    w3 = DummyWeb3()
    address = '0xabc'
    w3.codes[address] = b'\x01\x02'
    result = fetch_and_check.fetch_contract_bytecode(w3, address)
    assert result == "0102"


def test_random_contract_address_error():
    w3 = types.SimpleNamespace()
    w3.eth = types.SimpleNamespace(block_number=1, get_block=lambda *a, **k: types.SimpleNamespace(transactions=[]), get_code=lambda a: b'')
    with pytest.raises(RuntimeError):
        fetch_and_check.random_contract_address(w3, search_depth=1, attempts=1)
