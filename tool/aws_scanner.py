from __future__ import annotations

import argparse
import json
import logging
import os
import time
from typing import Callable, Optional

import pyarrow.dataset as ds

from data_getters import DataGetterAWSParquet
from fetch_and_check import run_checks


DEFAULT_PARQUET_DATASET = "s3://aws-public-blockchain/v1.0/eth/contracts/"
DEFAULT_STATE_FILE = "reports/aws_scanner_state.json"
DEFAULT_REPORT_FILE = "reports/aws_scan_results.jsonl"

logger = logging.getLogger(__name__)


def _latest_block(path: str) -> int:
    """Return the highest block number in the dataset."""
    dataset = ds.dataset(path, format="parquet")
    table = dataset.to_table(columns=["block_number"])
    return max(table["block_number"].to_pylist())


def _load_state(path: str) -> dict:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return {}


def _save_state(path: str, state: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(state, fh)


def scan_once(
    parquet_path: str,
    *,
    state_file: str = DEFAULT_STATE_FILE,
    report_file: str = DEFAULT_REPORT_FILE,
    batch_blocks: int = 1,
    page_rows: int = 2000,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> bool:
    """Process a single batch of contracts.

    Results are appended to ``report_file`` as JSON lines. Returns ``True`` if
    new contracts were processed.
    """
    state = _load_state(state_file)
    getter = DataGetterAWSParquet(parquet_path, page_rows=page_rows)
    latest = _latest_block(parquet_path)
    next_block = state.get("next_block")
    last_known = state.get("last_known_latest")
    if next_block is None:
        next_block = latest
        last_known = latest
    elif latest > (last_known or -1):
        next_block = latest
        last_known = latest
    start_block = max(next_block - batch_blocks + 1, 0)
    end_block = next_block
    if start_block > end_block:
        return False

    processed = 0
    os.makedirs(os.path.dirname(report_file), exist_ok=True)
    with open(report_file, "a", encoding="utf-8") as out:
        for page in getter.fetch_chunk(start_block, end_block):
            for row in page:
                res = run_checks(row["ByteCode"], row["Address"])
                entry = {
                    "address": row["Address"],
                    "block": row["BlockNumber"],
                    "suicidal": bool(res.get("suicidal")),
                    "prodigal": bool(res.get("prodigal")),
                    "greedy": bool(res.get("greedy")),
                }
                out.write(json.dumps(entry) + "\n")
                processed += 1
                if progress_cb:
                    progress_cb(
                        f"processed {processed} (block {row['BlockNumber']})"
                    )
    state["next_block"] = start_block - 1
    state["last_known_latest"] = last_known
    _save_state(state_file, state)
    return processed > 0


def run_continuous(
    parquet_path: str,
    *,
    interval: float = 5.0,
    batch_blocks: int = 1,
    state_file: str = DEFAULT_STATE_FILE,
    report_file: str = DEFAULT_REPORT_FILE,
    page_rows: int = 2000,
    max_rounds: Optional[int] = None,
) -> None:
    """Continuously scan the dataset until stopped."""
    rounds = 0
    while True:
        if max_rounds is not None and rounds >= max_rounds:
            break
        scan_once(
            parquet_path,
            state_file=state_file,
            report_file=report_file,
            batch_blocks=batch_blocks,
            page_rows=page_rows,
            progress_cb=logger.info,
        )
        rounds += 1
        time.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="Continuously scan AWS dataset")
    parser.add_argument(
        "dataset",
        nargs="?",
        default=DEFAULT_PARQUET_DATASET,
        help="Parquet dataset path",
    )
    parser.add_argument("--state-file", default=DEFAULT_STATE_FILE)
    parser.add_argument("--report-file", default=DEFAULT_REPORT_FILE)
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--batch-blocks", type=int, default=1)
    parser.add_argument("--page-rows", type=int, default=2000)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    run_continuous(
        args.dataset,
        interval=args.interval,
        batch_blocks=args.batch_blocks,
        state_file=args.state_file,
        report_file=args.report_file,
        page_rows=args.page_rows,
    )


if __name__ == "__main__":
    main()
