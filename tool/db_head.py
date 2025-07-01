from __future__ import annotations

import argparse
import sqlite3
from typing import List, Tuple


def get_entries(db_path: str, limit: int = 5) -> List[Tuple[str, str, int]]:
    """Return the first *limit* rows from the contracts table."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            "SELECT address, bytecode, block_number FROM contracts "
            "ORDER BY block_number LIMIT ?",
            (limit,),
        )
        return cur.fetchall()
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Show first rows of a contract DB")
    parser.add_argument("db", help="SQLite database file")
    parser.add_argument("--count", type=int, default=5,
                        help="number of rows to display (default: 5)")
    args = parser.parse_args()
    rows = get_entries(args.db, args.count)
    for address, bytecode, block in rows:
        snippet = bytecode[:20] + ("..." if len(bytecode) > 20 else "")
        print(f"{block:>10}  {address}  {snippet}")


if __name__ == "__main__":
    main()
