from __future__ import annotations

from typing import Iterable, List, Tuple

import pyarrow.dataset as ds

from .bigquery_getter import DataGetter


class DataGetterAWSParquet(DataGetter):
    """Load contract data from AWS Open-Data Parquet dumps.

    The dataset must contain ``address``, ``bytecode`` and ``block_number``
    columns. ``path`` can point to a local directory or an S3 bucket
    (e.g. ``s3://...``). Results are yielded in pages of ``page_rows``
    tuples ``(address, bytecode)``.
    """

    def __init__(self, path: str, page_rows: int = 20_000) -> None:
        self._dataset = ds.dataset(path, format="parquet")
        self._page_rows = page_rows

    def fetch_chunk(self, start_block: int, end_block: int) -> Iterable[List[Tuple[str, str]]]:
        filt = (
            (ds.field("block_number") >= start_block)
            & (ds.field("block_number") <= end_block)
        )
        table = self._dataset.to_table(filter=filt)
        rows = list(zip(table["address"].to_pylist(), table["bytecode"].to_pylist()))
        for i in range(0, len(rows), self._page_rows):
            yield rows[i : i + self._page_rows]
