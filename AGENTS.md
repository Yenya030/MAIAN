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
