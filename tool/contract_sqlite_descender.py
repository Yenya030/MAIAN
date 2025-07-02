from __future__ import annotations

import argparse
import os
import sqlite3
import time
import threading
from typing import Optional, Callable

from contract_sqlite_loader import (
    DEFAULT_PARQUET_DATASET,
    _init_db,
    _load_meta,
    _save_meta,
    _latest_block,
)
from data_getters import DataGetterAWSParquet

DEFAULT_PAGE_ROWS = 2000


def update_contract_db_reverse(
    db_path: str,
    *,
    size_limit_mb: float,
    parquet_path: str = DEFAULT_PARQUET_DATASET,
    page_rows: int = DEFAULT_PAGE_ROWS,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> None:
    """Fill *db_path* with contract data starting from the newest block.

    Contracts are fetched from *parquet_path* working backwards until the
    database reaches ``size_limit_mb`` megabytes or block 0 is reached.

    ``progress_cb`` is invoked with a short message after every inserted row
    to provide live feedback while the loader runs.
    """
    limit_bytes = int(size_limit_mb * 1024 * 1024)
    getter = DataGetterAWSParquet(parquet_path, page_rows=page_rows)
    conn = sqlite3.connect(db_path)
    try:
        _init_db(conn, limit_bytes)
        meta = _load_meta(conn)
        current = int(meta.get("lowest_block", _latest_block(parquet_path)))
        highest = meta.get("highest_block")
        if highest is None:
            meta["highest_block"] = str(current)
        inserted_count = 0
        while current >= 0 and os.path.getsize(db_path) < limit_bytes:
            start = max(current - page_rows + 1, 0)
            for page in getter.fetch_chunk(start, current):
                for row in page:
                    conn.execute(
                        "INSERT OR IGNORE INTO contracts(address, bytecode, block_number) VALUES(?, ?, ?)",
                        (row["Address"], row["ByteCode"], row["BlockNumber"]),
                    )
                    inserted_count += 1
                    if progress_cb is not None:
                        progress_cb(f"inserted {inserted_count} rows")
                conn.commit()
            current = start - 1
            meta["lowest_block"] = str(current + 1)
            _save_meta(conn, meta)
            if os.path.getsize(db_path) >= limit_bytes:
                break
    finally:
        conn.close()


def run(
    parquet_path: str,
    db_path: str,
    size_limit_mb: float,
    interval: float = 5.0,
    page_rows: int = DEFAULT_PAGE_ROWS,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> None:
    """Continuously update ``db_path`` until the size limit is reached.

    ``progress_cb`` receives status messages after each inserted row and before
    the loader sleeps between rounds.
    """
    while (
        not os.path.exists(db_path)
        or os.path.getsize(db_path) < size_limit_mb * 1024 * 1024
    ):
        update_contract_db_reverse(
            db_path,
            size_limit_mb=size_limit_mb,
            parquet_path=parquet_path,
            page_rows=page_rows,
            progress_cb=progress_cb,
        )
        if os.path.getsize(db_path) >= size_limit_mb * 1024 * 1024:
            break
        if progress_cb is not None:
            progress_cb("waiting for next round")
        time.sleep(interval)


class SimpleGUI:
    """Very small curses UI showing the current lowest block."""

    def __init__(self, parquet_path: str, db_path: str, size_limit_mb: float, interval: float = 5.0) -> None:
        self.parquet_path = parquet_path
        self.db_path = db_path
        self.size_limit_mb = size_limit_mb
        self.interval = interval
        self.status = "Starting..."

    def _log(self, msg: str) -> None:
        self.status = msg

    def _get_lowest(self) -> Optional[int]:
        if not os.path.exists(self.db_path):
            return None
        conn = sqlite3.connect(self.db_path)
        try:
            _init_db(conn, int(self.size_limit_mb * 1024 * 1024))
            meta = _load_meta(conn)
            val = meta.get("lowest_block")
            return int(val) if val is not None else None
        finally:
            conn.close()

    def _loop(self, stdscr: "curses._CursesWindow") -> None:
        import curses

        stdscr.nodelay(True)
        while True:
            lowest = self._get_lowest()
            stdscr.clear()
            stdscr.addstr(0, 0, "Simple SQL Loader (q to quit)")
            if lowest is not None:
                stdscr.addstr(2, 0, f"Lowest block: {lowest}")
            else:
                stdscr.addstr(2, 0, "No data yet")
            stdscr.addstr(4, 0, self.status)
            stdscr.refresh()
            ch = stdscr.getch()
            if ch == ord("q"):
                break
            time.sleep(0.5)

    def run(self) -> None:
        import curses

        curses.wrapper(self._loop)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download contracts into SQLite descending")
    parser.add_argument("--db", required=True, help="output SQLite database")
    parser.add_argument("--size-limit", type=float, required=True, help="database size limit in MB")
    parser.add_argument("--interval", type=float, default=5.0, help="update interval")
    parser.add_argument("--page-rows", type=int, default=DEFAULT_PAGE_ROWS, help="rows per fetch chunk")
    parser.add_argument("--gui", action="store_true", help="run with simple GUI")
    args = parser.parse_args()

    if args.gui:
        gui = SimpleGUI(
            DEFAULT_PARQUET_DATASET,
            args.db,
            args.size_limit,
            interval=args.interval,
        )
        run_thread = threading.Thread(
            target=run,
            args=(
                DEFAULT_PARQUET_DATASET,
                args.db,
                args.size_limit,
                args.interval,
                args.page_rows,
            ),
            kwargs={"progress_cb": gui._log},
        )
        run_thread.daemon = True
        run_thread.start()
        gui.run()
    else:
        run(
            DEFAULT_PARQUET_DATASET,
            args.db,
            args.size_limit,
            args.interval,
            args.page_rows,
            progress_cb=print,
        )


if __name__ == "__main__":
    main()

