from __future__ import annotations

from __future__ import annotations

import argparse
import json

from data_getters import DataGetterBigQuery


def download_contracts(
    dataset: str,
    start_block: int,
    end_block: int,
    *,
    output_file: str,
    page_rows: int = 2000,
) -> None:
    """Fetch contracts from BigQuery and save them to ``output_file``."""
    getter = DataGetterBigQuery(dataset, page_rows=page_rows)
    with open(output_file, "w", encoding="utf-8") as out:
        for page in getter.fetch_chunk(start_block, end_block):
            for row in page:
                out.write(json.dumps(row) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download contract bytecode from Google BigQuery"
    )
    parser.add_argument(
        "--dataset",
        default="bigquery-public-data.crypto_ethereum.contracts",
        help="BigQuery table with contract data",
    )
    parser.add_argument("--start-block", type=int, required=True)
    parser.add_argument("--end-block", type=int, required=True)
    parser.add_argument(
        "--output-file",
        default="contracts/bigquery_contracts.jsonl",
        help="destination JSONL file",
    )
    parser.add_argument("--page-rows", type=int, default=2000)
    args = parser.parse_args()

    download_contracts(
        args.dataset,
        args.start_block,
        args.end_block,
        output_file=args.output_file,
        page_rows=args.page_rows,
    )


if __name__ == "__main__":
    main()
