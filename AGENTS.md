This is a repository for Maian, a Python tool for detecting vulnerabilities in Ethereum smart contracts. Most code resides in the `tool` directory. Tests live in `tests` and use pytest.

Coding conventions:
- Python 3 code with standard library modules. Maintain compatibility with Python 3.8+.
- Use 4-space indentation.
- Keep functions small and documented with docstrings where practical.

Running tests:
- Install dependencies with `pip install web3 z3-solver`.
- Execute the full test suite with `pytest -q` from the repository root.

Unique scanning:
- The `fetch_and_check.py` utility saves scanned contract addresses to
  `reports/scanned_addresses.txt` by default. Duplicate addresses are skipped
  unless the `--allow-duplicates` flag is passed.
  Addresses flagged as vulnerable are appended to
  `reports/suicidal.txt`, `reports/prodigal.txt`, and `reports/greedy.txt`.

Any new scripts or modules should include simple unit tests under `tests/` and should avoid network calls during tests by using mocks.

Validation steps:
- Network fetch helpers now log request URLs, HTTP status codes, and response lengths.
- Contract data is validated before and after writing to detect truncation.
- Run `pytest -q` after installing dependencies to verify these checks.

Additional tools:

- `contract_stats.py` can save statistics about contracts to a CSV file. Run it with
  `--output-file <file>` to specify the destination (default `contract_stats.csv`).
  Use `--block-range <N>` to count contracts in the last `N` blocks ending at the latest block.
- `contract_downloader.py` can fetch bytecode from different sources and stores
  contracts in `contracts/contracts.jsonl` together with a metadata file.
  When no metadata exists and no block range is provided, the downloader uses
  the current `eth_blockNumber` as both start and end block.
- `db_checker.py` scans contracts stored in a SQLite database and writes a JSON
  report to `reports/db_scan_report.json` while showing live progress.
- `aws_scanner.py` reads contracts from the AWS Open-Data Parquet dump. It
  processes the newest 1000 blocks by default and prints live progress. Pass
  `--continuous` to keep scanning in a loop. Results are appended to
  `reports/aws_scan_results.jsonl` and progress is shown on one line. Adjust the
  block range with `--batch-blocks`. The scanner defaults to
  `s3://aws-public-blockchain/v1.0/eth/contracts/` when no dataset path is
  supplied.

- A `Dockerfile` in the repository root can build a container with all
  dependencies installed. Build it using `docker build -t maian .`.

