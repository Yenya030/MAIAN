from __future__ import annotations

import argparse
import os
import sqlite3
import time
from typing import Dict, Optional

import pyarrow.dataset as ds

from data_getters import DataGetterAWSParquet

DEFAULT_LIMIT_MB = 40


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
        for page in getter.fetch_chunk(start_block, end_block):
            for r in page:
                conn.execute(
                    "INSERT OR IGNORE INTO contracts(address, bytecode, block_number) VALUES(?, ?, ?)",
                    (r["Address"], r["ByteCode"], r["BlockNumber"]),
                )
                conn.commit()
                inserted = True
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
) -> None:
    """Continuously update the database until the size limit is reached."""
    while True:
        if os.path.exists(db_path) and os.path.getsize(db_path) >= size_limit_mb * 1024 * 1024:
            break
        inserted = update_contract_db(
            parquet_path,
            db_path,
            size_limit_mb=size_limit_mb,
            page_rows=page_rows,
        )
        if not inserted:
            break
        if os.path.getsize(db_path) >= size_limit_mb * 1024 * 1024:
            break
        time.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="Store contract data in SQLite")
    parser.add_argument("parquet", help="Path to Parquet dataset")
    parser.add_argument("db", help="SQLite database file")
    parser.add_argument("--page-rows", type=int, default=2000, help="rows per fetch chunk")
    parser.add_argument("--interval", type=float, default=5.0, help="poll interval for continuous mode")
    parser.add_argument("--size-limit", type=float, default=DEFAULT_LIMIT_MB, help="database size limit in MB")
    parser.add_argument("--once", action="store_true", help="run a single update instead of continuous mode")
    args = parser.parse_args()

    if args.once:
        update_contract_db(
            args.parquet,
            args.db,
            size_limit_mb=args.size_limit,
            page_rows=args.page_rows,
        )
    else:
        run_continuous(
            args.parquet,
            args.db,
            interval=args.interval,
            page_rows=args.page_rows,
            size_limit_mb=args.size_limit,
        )


if __name__ == "__main__":
    main()
