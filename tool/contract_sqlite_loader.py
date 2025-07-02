from __future__ import annotations

import argparse
import os
import sqlite3
import time
import threading
from typing import Callable, Dict, Optional

import pyarrow.dataset as ds

from data_getters import DataGetterAWSParquet

DEFAULT_LIMIT_MB = 40
# Default S3 path for the AWS Open Data Parquet dataset
DEFAULT_PARQUET_DATASET = "s3://aws-public-blockchain/v1.0/eth/contracts/"


def _latest_block(path: str) -> int:
    """Return the highest block number in the Parquet dataset."""
    dataset = ds.dataset(path, format="parquet")
    table = dataset.to_table(columns=["block_number"])
    return max(table["block_number"].to_pylist())


def _init_db(conn: sqlite3.Connection, size_limit: int) -> None:
    """Create tables if they do not exist and store size limit."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS contracts (
            address TEXT PRIMARY KEY,
            bytecode TEXT NOT NULL,
            block_number INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )
    cur = conn.execute("SELECT value FROM meta WHERE key='size_limit'")
    row = cur.fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO meta(key, value) VALUES('size_limit', ?)",
            (str(size_limit),),
        )
        conn.commit()


def _load_meta(conn: sqlite3.Connection) -> Dict[str, str]:
    cur = conn.execute("SELECT key, value FROM meta")
    return {k: v for k, v in cur.fetchall()}


def _save_meta(conn: sqlite3.Connection, meta: Dict[str, str]) -> None:
    for k, v in meta.items():
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)",
            (k, str(v)),
        )
    conn.commit()


def update_contract_db(
    parquet_path: str,
    db_path: str,
    *,
    size_limit_mb: Optional[float] = None,
    start_block: Optional[int] = None,
    end_block: Optional[int] = None,
    page_rows: int = 2000,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> bool:
    """Fetch new contracts from *parquet_path* and store them in *db_path*.

    Returns ``True`` if new rows were inserted.
    """
    limit = int((size_limit_mb or DEFAULT_LIMIT_MB) * 1024 * 1024)
    conn = sqlite3.connect(db_path)
    try:
        _init_db(conn, limit)
        meta = _load_meta(conn)
        if size_limit_mb is not None and meta.get("size_limit") != str(limit):
            meta["size_limit"] = str(limit)
        newest = int(meta.get("newest_block", -1)) if meta.get("newest_block") else None
        oldest = int(meta.get("oldest_block", -1)) if meta.get("oldest_block") else None

        getter = DataGetterAWSParquet(parquet_path, page_rows=page_rows)
        latest = _latest_block(parquet_path)

        if start_block is None and end_block is None:
            if newest is None:
                start_block = latest
                end_block = latest
            else:
                start_block = newest + 1
                end_block = latest
        elif start_block is None:
            start_block = end_block
        elif end_block is None:
            end_block = start_block

        if newest is not None and start_block <= newest <= end_block:
            start_block = newest + 1
        if oldest is not None and start_block <= oldest <= end_block:
            end_block = oldest - 1
        if start_block > end_block:
            _save_meta(conn, meta)
            return False

        inserted = False
        inserted_count = 0
        for page in getter.fetch_chunk(start_block, end_block):
            for r in page:
                conn.execute(
                    "INSERT OR IGNORE INTO contracts(address, bytecode, block_number) VALUES(?, ?, ?)",
                    (r["Address"], r["ByteCode"], r["BlockNumber"]),
                )
                conn.commit()
                inserted = True
                inserted_count += 1
                if progress_cb is not None:
                    progress_cb(f"inserted {inserted_count} rows")
                if os.path.getsize(db_path) >= limit:
                    break
            if os.path.getsize(db_path) >= limit:
                break
        if inserted:
            cur = conn.execute("SELECT MAX(block_number), MIN(block_number) FROM contracts")
            newest_db, oldest_db = cur.fetchone()
            meta["newest_block"] = str(newest_db)
            meta["oldest_block"] = str(oldest_db)
        _save_meta(conn, meta)
        return inserted
    finally:
        conn.close()


def run_continuous(
    parquet_path: str,
    db_path: str,
    *,
    interval: float = 5.0,
    page_rows: int = 2000,
    size_limit_mb: float = DEFAULT_LIMIT_MB,
    max_rounds: Optional[int] = None,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> None:
    """Continuously update the database until the size limit is reached.

    Parameters
    ----------
    parquet_path:
        Location of the Parquet dataset.
    db_path:
        Destination SQLite file.
    interval:
        Sleep time between update rounds.
    page_rows:
        Number of rows fetched per chunk.
    size_limit_mb:
        Maximum allowed database size in MB.
    max_rounds:
        Optional limit on how many update cycles to run. Mainly useful for
        testing.
    progress_cb:
        Optional callback invoked with a progress message after each inserted
        row.
    """

    rounds = 0
    limit_bytes = size_limit_mb * 1024 * 1024
    while True:
        if max_rounds is not None and rounds >= max_rounds:
            break
        if os.path.exists(db_path) and os.path.getsize(db_path) >= limit_bytes:
            break
        inserted = update_contract_db(
            parquet_path,
            db_path,
            size_limit_mb=size_limit_mb,
            page_rows=page_rows,
            progress_cb=progress_cb,
        )
        if os.path.exists(db_path) and os.path.getsize(db_path) >= limit_bytes:
            break
        rounds += 1
        time.sleep(interval)


def run_continuous_until(
    parquet_path: str,
    db_path: str,
    stop_event: threading.Event,
    *,
    interval: float = 5.0,
    page_rows: int = 2000,
    size_limit_mb: float = DEFAULT_LIMIT_MB,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> None:
    """Run continuous updates until ``stop_event`` is set or size limit is hit.

    Parameters
    ----------
    progress_cb:
        Optional callback invoked with a progress message after each inserted
        row.
    """

    limit_bytes = size_limit_mb * 1024 * 1024
    while not stop_event.is_set():
        if os.path.exists(db_path) and os.path.getsize(db_path) >= limit_bytes:
            break
        update_contract_db(
            parquet_path,
            db_path,
            size_limit_mb=size_limit_mb,
            page_rows=page_rows,
            progress_cb=progress_cb,
        )
        if os.path.exists(db_path) and os.path.getsize(db_path) >= limit_bytes:
            break
        if stop_event.wait(interval):
            break


def main() -> None:
    parser = argparse.ArgumentParser(description="Store contract data in SQLite")
    parser.add_argument(
        "paths",
        nargs="+",
        help=(
            "<db> or <dataset> <db>. When only <db> is given, the AWS "
            f"dataset {DEFAULT_PARQUET_DATASET} is used"
        ),
    )
    parser.add_argument("--page-rows", type=int, default=2000, help="rows per fetch chunk")
    parser.add_argument("--interval", type=float, default=5.0, help="poll interval for continuous mode")
    parser.add_argument("--size-limit", type=float, default=DEFAULT_LIMIT_MB, help="database size limit in MB")
    parser.add_argument("--once", action="store_true", help="run a single update instead of continuous mode")
    parser.add_argument("--gui", action="store_true", help="launch simple GUI")
    args = parser.parse_args()
    if len(args.paths) == 1:
        parquet_path = DEFAULT_PARQUET_DATASET
        db_path = args.paths[0]
    elif len(args.paths) == 2:
        parquet_path, db_path = args.paths
    else:
        parser.error("expected <db> or <dataset> <db>")

    if args.gui:
        from sql_gui import run_terminal_app

        run_terminal_app(parquet_path, db_path, interval=args.interval)
    elif args.once:
        update_contract_db(
            parquet_path,
            db_path,
            size_limit_mb=args.size_limit,
            page_rows=args.page_rows,
            progress_cb=print,
        )
    else:
        run_continuous(
            parquet_path,
            db_path,
            interval=args.interval,
            page_rows=args.page_rows,
            size_limit_mb=args.size_limit,
            progress_cb=print,
        )


if __name__ == "__main__":
    main()
