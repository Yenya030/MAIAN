import argparse
import threading
import tkinter as tk
from typing import Optional

from contract_sqlite_loader import (
    DEFAULT_PARQUET_DATASET,
    run_continuous_until,
    _load_meta,
    _init_db,
)
import sqlite3
import os


class LoaderApp(tk.Tk):
    def __init__(self, parquet_path: str, db_path: str, interval: float = 5.0) -> None:
        super().__init__()
        self.parquet_path = parquet_path
        self.db_path = db_path
        self.interval = interval
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.title("SQLite Loader")
        self.label_var = tk.StringVar(value="Starting...")
        tk.Label(self, textvariable=self.label_var).pack(padx=10, pady=10)
        tk.Button(self, text="Kill", command=self._kill).pack(pady=5)
        self.protocol("WM_DELETE_WINDOW", self._kill)
        self.after(100, self._start)

    def _start(self) -> None:
        self.thread = threading.Thread(
            target=run_continuous_until,
            args=(self.parquet_path, self.db_path, self.stop_event),
            kwargs={"interval": self.interval},
            daemon=True,
        )
        self.thread.start()
        self.after(500, self._update_status)

    def _update_status(self) -> None:
        if os.path.exists(self.db_path):
            conn = sqlite3.connect(self.db_path)
            try:
                _init_db(conn, int(1))  # ensure meta table exists
                meta = _load_meta(conn)
            finally:
                conn.close()
            newest = meta.get("newest_block")
            oldest = meta.get("oldest_block")
            if newest and oldest:
                self.label_var.set(f"Blocks {oldest}-{newest}")
            else:
                self.label_var.set("Waiting for blocks...")
        if not self.stop_event.is_set():
            self.after(1000, self._update_status)

    def _kill(self) -> None:
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=1)
        self.destroy()


def main() -> None:
    parser = argparse.ArgumentParser(description="GUI for contract_sqlite_loader")
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

    app = LoaderApp(parquet_path, db_path, interval=args.interval)
    app.mainloop()


if __name__ == "__main__":
    main()
