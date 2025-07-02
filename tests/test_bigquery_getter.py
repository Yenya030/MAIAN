from pathlib import Path
import sys

root_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root_dir / 'tool'))

from data_getters import DataGetterBigQuery


class DummyRow:
    def __init__(self, address, bytecode, block_number):
        self.address = address
        self.bytecode = bytecode
        self.block_number = block_number


class DummyJob:
    def __init__(self, rows):
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)


class DummyClient:
    def __init__(self, rows):
        self.rows = rows
        self.query_args = None

    def query(self, sql, job_config=None):
        self.query_args = (sql, job_config.query_parameters)
        return DummyJob(self.rows)


def test_bigquery_getter_basic():
    rows = [DummyRow('0x1', 'aa', 1), DummyRow('0x2', 'bb', 2)]
    client = DummyClient(rows)
    getter = DataGetterBigQuery('dataset.table', page_rows=1, client=client)
    pages = list(getter.fetch_chunk(1, 2))
    assert pages == [[
        {'Address': '0x1', 'ByteCode': 'aa', 'BlockNumber': 1}
    ], [
        {'Address': '0x2', 'ByteCode': 'bb', 'BlockNumber': 2}
    ]]
    sql, params = client.query_args
    assert 'dataset.table' in sql
    assert len(params) == 2

