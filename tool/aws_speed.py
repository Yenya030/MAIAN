from __future__ import annotations

import argparse
import time
from typing import Dict

from contract_sqlite_loader import DEFAULT_PARQUET_DATASET
from aws_scanner import _latest_block
from data_getters import DataGetterAWSParquet


def measure_speed(
    dataset: str,
    start_block: int,
    end_block: int,
    *,
    page_rows: int = 2000,
) -> Dict[str, float]:
    """Return download statistics for the given block range.

    The function measures how many contracts and raw bytes are
    retrieved from ``dataset`` between ``start_block`` and ``end_block``.
    ``page_rows`` controls the page size for :class:`DataGetterAWSParquet`.
    """
    getter = DataGetterAWSParquet(dataset, page_rows=page_rows)
    total_bytes = 0
    total_contracts = 0
    t_start = time.time()
    for page in getter.fetch_chunk(start_block, end_block):
        for row in page:
            total_bytes += len(bytes.fromhex(row["ByteCode"]))
            total_contracts += 1
    elapsed = time.time() - t_start
    mb_per_s = (total_bytes / 1_000_000) / elapsed if elapsed else 0.0
    return {
        "contracts": total_contracts,
        "bytes": total_bytes,
        "seconds": elapsed,
        "mb_per_second": mb_per_s,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Measure download speed for the AWS Parquet dataset"
    )
    parser.add_argument(
        "dataset",
        nargs="?",
        default=DEFAULT_PARQUET_DATASET,
        help="Parquet dataset path",
    )
    parser.add_argument(
        "--blocks",
        type=int,
        default=1000,
        help="number of newest blocks to fetch",
    )
    parser.add_argument("--page-rows", type=int, default=2000)
    args = parser.parse_args()

    end_block = _latest_block(args.dataset)
    start_block = max(end_block - args.blocks + 1, 0)
    stats = measure_speed(
        args.dataset,
        start_block,
        end_block,
        page_rows=args.page_rows,
    )
    print(
        f"Fetched {stats['contracts']} contracts "
        f"({stats['bytes']} bytes) in {stats['seconds']:.2f}s -> "
        f"{stats['mb_per_second']:.2f} MB/s"
    )


if __name__ == "__main__":
    main()
