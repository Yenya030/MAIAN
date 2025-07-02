from __future__ import annotations

import argparse
import json
import sqlite3
from typing import Callable, Dict, Optional

from fetch_and_check import run_checks


ProgressCB = Optional[Callable[[str], None]]


def scan_database(
    db_path: str,
    *,
    limit: int | None = None,
    progress_cb: ProgressCB = None,
) -> Dict[str, int]:
    """Run Maian checks on contracts stored in ``db_path``.

    Parameters
    ----------
    db_path:
        SQLite database containing a ``contracts`` table with ``address`` and
        ``bytecode`` columns.
    limit:
        Optional maximum number of contracts to scan.
    progress_cb:
        Optional callback invoked after each contract is processed. Receives a
        human readable progress string.
    """
    conn = sqlite3.connect(db_path)
    try:
        total = conn.execute("SELECT COUNT(*) FROM contracts").fetchone()[0]
        cur = conn.execute("SELECT address, bytecode FROM contracts")
        scanned = 0
        flagged = {"suicidal": 0, "prodigal": 0, "greedy": 0}
        for address, bytecode in cur:
            res = run_checks(bytecode, address)
            scanned += 1
            for key in flagged:
                if res.get(key):
                    flagged[key] += 1
            if progress_cb:
                remaining = max(total - scanned, 0)
                progress_cb(
                    f"{scanned}/{total} scanned - "
                    f"S:{flagged['suicidal']} P:{flagged['prodigal']} "
                    f"G:{flagged['greedy']} left:{remaining}"
                )
            if limit is not None and scanned >= limit:
                break
    finally:
        conn.close()
    result = {"total": total, "scanned": scanned}
    result.update(flagged)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Maian checks on a contract database")
    parser.add_argument("--db", required=True, help="SQLite database with contracts")
    parser.add_argument(
        "--limit",
        type=int,
        help="optional limit on number of contracts to scan",
    )
    parser.add_argument(
        "--report",
        default="reports/db_scan_report.json",
        help="output file for the JSON report",
    )
    args = parser.parse_args()
    summary = scan_database(args.db, limit=args.limit, progress_cb=print)
    with open(args.report, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)
    print(f"Report written to {args.report}")


if __name__ == "__main__":
    main()
