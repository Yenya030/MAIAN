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
from contract_sqlite_loader import DEFAULT_PARQUET_DATASET

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


def make_live_progress() -> Callable[[str], None]:
    """Return a progress callback that prints updates on one line."""

    def _cb(msg: str) -> None:
        print(f"\r{msg}", end="", flush=True)

    return _cb


def scan_once(
    parquet_path: str,
    *,
    state_file: str = DEFAULT_STATE_FILE,
    report_file: str = DEFAULT_REPORT_FILE,
    batch_blocks: int = 1000,
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

    logger.info("retrieving blocks %d-%d", start_block, end_block)
    t_fetch = time.time()
    pages = list(getter.fetch_chunk(start_block, end_block))
    fetch_time = time.time() - t_fetch
    num_blocks = end_block - start_block + 1
    num_contracts = sum(len(p) for p in pages)
    logger.info(
        "retrieved %d blocks with %d contracts in %.2fs",
        num_blocks,
        num_contracts,
        fetch_time,
    )
    logger.info("scanning %d contracts", num_contracts)

    processed = 0
    t_scan = time.time()
    os.makedirs(os.path.dirname(report_file), exist_ok=True)
    with open(report_file, "a", encoding="utf-8") as out:
        for page in pages:
            for row in page:
                res = run_checks(row["ByteCode"], row["Address"])
                entry = {
                    "address": row["Address"],
                    "block": row["BlockNumber"],
                    "suicidal": bool(res.get("suicidal")),
                    "prodigal": bool(res.get("prodigal")),
                    "greedy": bool(res.get("greedy")),
                }
                if entry["suicidal"] or entry["prodigal"] or entry["greedy"]:
                    out.write(json.dumps(entry) + "\n")
                    logger.info(
                        "vulnerable address %s at block %d",
                        entry["address"],
                        entry["block"],
                    )
                processed += 1
                if progress_cb:
                    progress_cb(
                        f"processed {processed} (block {row['BlockNumber']})"
                    )
    scan_time = time.time() - t_scan
    logger.info(
        "completed scan of %d contracts from %d blocks in %.2fs",
        num_contracts,
        num_blocks,
        scan_time,
    )
    if progress_cb:
        print()
    state["next_block"] = start_block - 1
    state["last_known_latest"] = last_known
    _save_state(state_file, state)
    return processed > 0


def run_continuous(
    parquet_path: str,
    *,
    interval: float = 0.0,
    batch_blocks: int = 1000,
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
            progress_cb=make_live_progress(),
        )
        rounds += 1
        if interval > 0:
            time.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="Scan AWS contract data")
    parser.add_argument(
        "dataset",
        nargs="?",
        default=DEFAULT_PARQUET_DATASET,
        help="Parquet dataset path",
    )
    parser.add_argument("--state-file", default=DEFAULT_STATE_FILE)
    parser.add_argument("--report-file", default=DEFAULT_REPORT_FILE)
    parser.add_argument("--interval", type=float, default=0.0)
    parser.add_argument(
        "--batch-blocks", type=int, default=1000,
        help="number of blocks to scan per iteration"
    )
    parser.add_argument("--page-rows", type=int, default=2000)
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="keep scanning in a loop instead of running once",
    )
    parser.add_argument(
        "--max-rounds",
        type=int,
        default=None,
        help="maximum number of scan iterations in continuous mode",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    if args.continuous:
        run_continuous(
            args.dataset,
            interval=args.interval,
            batch_blocks=args.batch_blocks,
            state_file=args.state_file,
            report_file=args.report_file,
            page_rows=args.page_rows,
            max_rounds=args.max_rounds,
        )
    else:
        scan_once(
            args.dataset,
            state_file=args.state_file,
            report_file=args.report_file,
            batch_blocks=args.batch_blocks,
            page_rows=args.page_rows,
            progress_cb=make_live_progress(),
        )


if __name__ == "__main__":
    main()
