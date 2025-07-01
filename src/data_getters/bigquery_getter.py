from __future__ import annotations

import os
import time
from typing import Iterable, List, Tuple, Optional

from google.cloud import bigquery
from google.api_core.exceptions import BadRequest, GoogleAPICallError
from google.api_core.client_options import ClientOptions


class DataGetter:
    """Base class for data getters."""

    def fetch_chunk(self, start_block: int, end_block: int) -> Iterable[List[Tuple[str, str]]]:
        raise NotImplementedError


class DataGetterBigQuery(DataGetter):
    """Fetch contract bytecodes from the public BigQuery Ethereum dataset."""

    _SQL = (
        """
        SELECT ANY_VALUE(address) AS address, bytecode
        FROM `bigquery-public-data.crypto_ethereum.contracts`
        WHERE block_number BETWEEN @start AND @end
          AND BYTE_LENGTH(bytecode) > 2000
          AND NOT REGEXP_CONTAINS(bytecode, r"^0x363d3d373d3d3d363d73|^0x3660008037600080")
        GROUP BY bytecode
        """
    )

    def __init__(
        self,
        page_rows: int = 20_000,
        *,
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
    ) -> None:
        """Create a new BigQuery data getter.

        ``api_key`` and ``project_id`` can be used to authenticate with an API
        key instead of ``GOOGLE_APPLICATION_CREDENTIALS``. If omitted, the
        default credentials flow is used.
        """
        if api_key is None:
            api_key = os.getenv("BIGQUERY_API_KEY")
        if project_id is None:
            project_id = os.getenv("BIGQUERY_PROJECT_ID")
        if api_key:
            opts = ClientOptions(api_key=api_key)
            self._client = bigquery.Client(project=project_id, client_options=opts)
        else:
            self._client = bigquery.Client()
        self._page_rows = page_rows
        self._last_job: Optional[bigquery.job.QueryJob] = None

    @property
    def last_job(self) -> Optional[bigquery.job.QueryJob]:
        """Return the last executed BigQuery job."""
        return self._last_job

    def _run_query(self, job_config: bigquery.QueryJobConfig) -> bigquery.job.QueryJob:
        delay = 2.0
        exc: Optional[Exception] = None
        for _ in range(5):
            try:
                job = self._client.query(self._SQL, job_config=job_config)
                return job
            except (BadRequest, GoogleAPICallError) as e:  # pragma: no cover - network
                exc = e
                time.sleep(delay)
                delay *= 2
        assert exc is not None
        raise exc

    def fetch_chunk(self, start_block: int, end_block: int) -> Iterable[List[Tuple[str, str]]]:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start", "INT64", start_block),
                bigquery.ScalarQueryParameter("end", "INT64", end_block),
            ]
        )
        job = self._run_query(job_config)
        self._last_job = job

        for page in job.result(page_size=self._page_rows).pages:
            yield [(r.address, r.bytecode) for r in page]
