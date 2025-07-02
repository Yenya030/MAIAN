from __future__ import annotations

from typing import Any, Dict, Iterable, List

from google.cloud import bigquery

from .base import DataGetter


class DataGetterBigQuery(DataGetter):
    """Load contract data from Google BigQuery.

    Parameters
    ----------
    dataset:
        Fully qualified BigQuery table to query. The table must provide
        ``address``, ``bytecode`` and ``block_number`` columns.
    page_rows:
        Number of rows to yield per page.
    client:
        Optional :class:`google.cloud.bigquery.Client` instance. One is created
        automatically when not provided.
    """

    def __init__(
        self,
        dataset: str = "bigquery-public-data.crypto_ethereum.contracts",
        *,
        page_rows: int = 2000,
        client: bigquery.Client | None = None,
    ) -> None:
        self._dataset = dataset
        self._page_rows = page_rows
        self._client = client or bigquery.Client()

    def fetch_chunk(
        self, start_block: int, end_block: int
    ) -> Iterable[List[Dict[str, Any]]]:
        query = (
            "SELECT address, bytecode, block_number "
            f"FROM `{self._dataset}` "
            "WHERE block_number >= @start AND block_number <= @end "
            "ORDER BY block_number"
        )
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start", "INT64", start_block),
                bigquery.ScalarQueryParameter("end", "INT64", end_block),
            ]
        )
        job = self._client.query(query, job_config=job_config)
        rows = [
            {
                "Address": r.address,
                "ByteCode": r.bytecode,
                "BlockNumber": r.block_number,
            }
            for r in job
        ]
        for i in range(0, len(rows), self._page_rows):
            yield rows[i : i + self._page_rows]
