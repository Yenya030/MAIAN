from __future__ import annotations

import argparse
import sqlite3
from typing import Callable, Optional, Dict

from fetch_and_check import run_checks

ProgressCB = Optional[Callable[[str], None]]


def scan_for_leaks(
    src_db: str,
    dst_db: str,
    *,
    limit: Optional[int] = None,
    progress_cb: ProgressCB = None,
) -> Dict[str, int]:
    """Scan unchecked contracts in ``src_db`` for leak vulnerabilities.

    Contracts marked as vulnerable are inserted into ``dst_db``.
    Returns a summary with the number of scanned and vulnerable entries.
    """
    src_conn = sqlite3.connect(src_db)
    dst_conn = sqlite3.connect(dst_db)
    try:
        dst_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS contracts (
                address TEXT PRIMARY KEY,
                bytecode TEXT NOT NULL,
                block_number INTEGER NOT NULL
            )
            """
        )
        dst_conn.commit()

        total = src_conn.execute(
            "SELECT COUNT(*) FROM contracts WHERE checked IS NULL OR checked=0"
        ).fetchone()[0]
        cur = src_conn.execute(
            "SELECT address, block_number, bytecode FROM contracts "
            "WHERE checked IS NULL OR checked=0"
        )
        scanned = 0
        vulnerable = 0
        for address, block, bytecode in cur:
            res = run_checks(bytecode, address)
            if res.get("prodigal"):
                dst_conn.execute(
                    "INSERT OR REPLACE INTO contracts(address, bytecode, block_number) "
                    "VALUES(?, ?, ?)",
                    (address, bytecode, block),
                )
                dst_conn.commit()
                vulnerable += 1
            src_conn.execute(
                "UPDATE contracts SET checked=1 WHERE address=?",
                (address,),
            )
            src_conn.commit()
            scanned += 1
            if progress_cb:
                progress_cb(f"{scanned}/{total} scanned")
            if limit is not None and scanned >= limit:
                break
    finally:
        src_conn.close()
        dst_conn.close()
    return {"scanned": scanned, "vulnerable": vulnerable, "total": total}


def main() -> None:
    parser = argparse.ArgumentParser(description="Scan database for leak vulnerabilities")
    parser.add_argument("--src-db", required=True, help="SQLite database with contracts")
    parser.add_argument("--dst-db", required=True, help="SQLite database for vulnerable contracts")
    parser.add_argument("--limit", type=int, help="optional limit on number of contracts to scan")
    args = parser.parse_args()
    summary = scan_for_leaks(args.src_db, args.dst_db, limit=args.limit, progress_cb=print)
    print()
    print(summary)


if __name__ == "__main__":
    main()
