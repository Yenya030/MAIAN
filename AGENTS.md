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
- `contract_downloader.py` can fetch bytecode from different sources. When using
  the Etherscan API it stores contracts in `contracts/contracts.jsonl` and
  tracks the block range in `contracts/metadata.json`. Verified contracts are
  written to `contracts/EtherscanVerified.jsonl`. A second source reads blocks
  from an RPC endpoint (for example Infura) and follows the same metadata
  scheme.
  When no metadata exists and no block range is provided, the downloader uses
  the current `eth_blockNumber` as both start and end block.
- Sample outputs used in the tests live in `contracts/etherscan.jsonl` and
  `contracts/infura.jsonl` alongside their metadata files.

- A `Dockerfile` in the repository root can build a container with all
  dependencies installed. Build it using `docker build -t maian .`.

