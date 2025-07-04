from __future__ import annotations

import argparse
import sqlite3
from typing import List, Tuple


def fetch_rows(db_path: str, limit: int = 5) -> List[Tuple[str, str, int]]:
    """Return up to *limit* rows from the contracts table ordered by block number."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            "SELECT address, bytecode, block_number FROM contracts ORDER BY block_number LIMIT ?",
            (limit,),
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return rows


def format_rows(rows: List[Tuple[str, str, int]]) -> str:
    """Format rows as a simple table for terminal output."""
    if not rows:
        return ""
    header = ("Address", "Block", "Bytecode")
    widths = [len(h) for h in header]
    for addr, bytecode, block in rows:
        widths[0] = max(widths[0], len(addr))
        widths[1] = max(widths[1], len(str(block)))
        widths[2] = max(widths[2], min(len(bytecode), 10))
    lines = [f"{header[0].ljust(widths[0])} {header[1].ljust(widths[1])} {header[2]}",
             "-" * (widths[0] + widths[1] + widths[2] + 2)]
    for addr, bytecode, block in rows:
        short_code = bytecode[:10]
        lines.append(f"{addr.ljust(widths[0])} {str(block).ljust(widths[1])} {short_code}")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Show first entries from a contract database")
    parser.add_argument("db", help="SQLite database file")
    parser.add_argument(
        "-n", "--limit", type=int, default=5, help="number of entries to show (default: 5)"
    )
    args = parser.parse_args()

    rows = fetch_rows(args.db, args.limit)
    print(format_rows(rows))


if __name__ == "__main__":
    main()
