"""Run Maian checks on contracts fetched via a Java BigQuery client."""
from __future__ import annotations

import argparse
import json
import subprocess
from typing import Dict, List

from fetch_and_check import run_checks


def run_java_fetcher(
    jar_path: str,
    dataset: str,
    start_block: int,
    end_block: int,
    output_file: str,
) -> None:
    """Execute the Java fetcher to retrieve contract data."""
    cmd = [
        "java",
        "-cp",
        jar_path,
        "BigQueryFetcher",
        "--dataset",
        dataset,
        "--start-block",
        str(start_block),
        "--end-block",
        str(end_block),
        "--output",
        output_file,
    ]
    subprocess.run(cmd, check=True)


def scan_bigquery_with_java(
    dataset: str,
    start_block: int,
    end_block: int,
    *,
    jar_path: str = "tool/java",
    output_file: str = "contracts/java_bigquery_contracts.jsonl",
) -> List[Dict[str, object]]:
    """Fetch contracts with the Java tool and run Maian checks."""
    run_java_fetcher(jar_path, dataset, start_block, end_block, output_file)
    reports: List[Dict[str, object]] = []
    with open(output_file, "r", encoding="utf-8") as fh:
        for line in fh:
            row = json.loads(line)
            res = run_checks(row["ByteCode"], row["Address"])
            res["address"] = row["Address"]
            res["block_number"] = row["BlockNumber"]
            reports.append(res)
    return reports


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch contracts from BigQuery using a Java helper and run Maian checks"
    )
    parser.add_argument(
        "--dataset",
        default="bigquery-public-data.crypto_ethereum.contracts",
        help="BigQuery table with contract data",
    )
    parser.add_argument("--start-block", type=int, required=True)
    parser.add_argument("--end-block", type=int, required=True)
    parser.add_argument(
        "--jar-path",
        default="tool/java",
        help="path to compiled BigQueryFetcher class or jar",
    )
    parser.add_argument(
        "--output-file",
        default="contracts/java_bigquery_contracts.jsonl",
        help="destination JSONL file",
    )
    args = parser.parse_args()
    reports = scan_bigquery_with_java(
        args.dataset,
        args.start_block,
        args.end_block,
        jar_path=args.jar_path,
        output_file=args.output_file,
    )
    for rep in reports:
        print(json.dumps(rep))


if __name__ == "__main__":
    main()
