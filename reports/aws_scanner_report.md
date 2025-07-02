# AWS Scanner Report

The `aws_scanner.py` utility scans smart contract bytecode from the AWS
Open-Data Parquet dump. It keeps a small JSON state to resume from the
latest processed block and appends results as JSON lines. Each iteration
inspects a range of blocks (1000 by default) starting from the highest block
number and moving backwards.

## Design
- Uses `DataGetterAWSParquet` to read contracts from Parquet files.
- Calls `run_checks` on each contract and records the vulnerability flags.
- State is stored in `reports/aws_scanner_state.json` with the next block to
  inspect and the last known highest block number.
- Progress output is printed on a single line using a callback returned by
  `make_live_progress`.

## Usage
Run a single scan of the newest 1000 blocks:

```bash
python tool/aws_scanner.py                 # uses default dataset
python tool/aws_scanner.py s3://bucket/path  # custom path
```
The default dataset path is
`s3://aws-public-blockchain/v1.0/eth/contracts/`.

Add `--continuous` to keep scanning in a loop. Each pass handles 1000 blocks by
default. Change this with `--batch-blocks` and use `--max-rounds` to limit the
number of loops. Progress is printed on stderr for each contract. Pass
`--verbose` to see the detailed output from the checks.
