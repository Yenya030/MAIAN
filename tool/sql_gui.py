import argparse
import curses
import os
import sqlite3
import threading
import time
from typing import Optional

from contract_sqlite_loader import (
    DEFAULT_PARQUET_DATASET,
    _init_db,
    _load_meta,
    run_continuous_until,
)


class TerminalLoaderApp:
    """Simple curses based interface for the SQLite loader."""

    def __init__(self, parquet_path: str, db_path: str, interval: float = 5.0) -> None:
        self.parquet_path = parquet_path
        self.db_path = db_path
        self.interval = interval
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.status = "Starting..."

    def _log(self, msg: str) -> None:
        self.status = msg

    def _run_loader(self) -> None:
        run_continuous_until(
            self.parquet_path,
            self.db_path,
            self.stop_event,
            interval=self.interval,
            progress_cb=self._log,
        )

    def _update_status_from_db(self) -> None:
        if os.path.exists(self.db_path):
            conn = sqlite3.connect(self.db_path)
            try:
                _init_db(conn, 1)
                meta = _load_meta(conn)
            finally:
                conn.close()
            newest = meta.get("newest_block")
            oldest = meta.get("oldest_block")
            if newest and oldest:
                self.status = f"Blocks {oldest}-{newest}"
            else:
                self.status = "Waiting for blocks..."

    def run(self, stdscr: "curses._CursesWindow") -> None:
        stdscr.nodelay(True)
        self.thread = threading.Thread(target=self._run_loader, daemon=True)
        self.thread.start()
        try:
            while not self.stop_event.is_set():
                self._update_status_from_db()
                stdscr.clear()
                stdscr.addstr(0, 0, "SQLite Loader (press q to quit)")
                stdscr.addstr(2, 0, self.status)
                stdscr.refresh()
                ch = stdscr.getch()
                if ch == ord("q"):
                    self.stop_event.set()
                    break
                time.sleep(0.5)
        finally:
            self.stop_event.set()
            if self.thread is not None:
                self.thread.join(timeout=1)


def run_terminal_app(parquet_path: str, db_path: str, interval: float = 5.0) -> None:
    """Launch the curses based loader UI."""

    curses.wrapper(lambda stdscr: TerminalLoaderApp(parquet_path, db_path, interval).run(stdscr))


def main() -> None:
    parser = argparse.ArgumentParser(description="Terminal GUI for contract_sqlite_loader")
    parser.add_argument(
        "paths",
        nargs="+",
        help=(
            "<db> or <dataset> <db>. When only <db> is given, the AWS "
            f"dataset {DEFAULT_PARQUET_DATASET} is used"
        ),
    )
    parser.add_argument("--interval", type=float, default=5.0, help="poll interval")
    args = parser.parse_args()

    if len(args.paths) == 1:
        parquet_path = DEFAULT_PARQUET_DATASET
        db_path = args.paths[0]
    elif len(args.paths) == 2:
        parquet_path, db_path = args.paths
    else:
        parser.error("expected <db> or <dataset> <db>")

    run_terminal_app(parquet_path, db_path, interval=args.interval)


if __name__ == "__main__":
    main()
