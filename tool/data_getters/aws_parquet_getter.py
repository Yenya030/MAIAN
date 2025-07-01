from __future__ import annotations

from typing import Any, Dict, Iterable, List

import pyarrow.dataset as ds

from .base import DataGetter


class DataGetterAWSParquet(DataGetter):
    """Load contract data from AWS Open-Data Parquet dumps.

    The dataset must contain ``address``, ``bytecode`` and ``block_number``
    columns. ``path`` can point to a local directory or an S3 bucket
    (e.g. ``s3://...``). Results are yielded in pages of ``page_rows``
    dictionaries with ``Address``, ``ByteCode`` and ``BlockNumber`` fields.
    """

    def __init__(self, path: str, page_rows: int = 20_000) -> None:
        self._dataset = ds.dataset(path, format="parquet")
        self._page_rows = page_rows

    def fetch_chunk(
        self, start_block: int, end_block: int
    ) -> Iterable[List[Dict[str, Any]]]:
        filt = (
            (ds.field("block_number") >= start_block)
            & (ds.field("block_number") <= end_block)
        )
        table = self._dataset.to_table(filter=filt)
        rows = [
            {
                "Address": addr,
                "ByteCode": code,
                "BlockNumber": blk,
            }
            for addr, code, blk in zip(
                table["address"].to_pylist(),
                table["bytecode"].to_pylist(),
                table["block_number"].to_pylist(),
            )
        ]
        for i in range(0, len(rows), self._page_rows):
            yield rows[i : i + self._page_rows]
