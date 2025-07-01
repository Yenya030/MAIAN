# Maian 

The repository contains Python implementation of Maian -- a tool for automatic detection of buggy Ethereum smart contracts of three different types: prodigal, suicidal and greedy. Maian processes contract's bytecode and tries to build a trace of transactions to find and confirm bugs. The technical aspects of the approach are described in [our paper](https://arxiv.org/abs/1802.06038). 

## Evaluating Contracts
Maian analyzes smart contracts defined in a file `<contract file>` with:  

1. Solidity source code, use `-s <contract file> <main contract name>`
2. Bytecode source, use `-bs <contract file>`
3. Bytecode compiled (i.e. the code sitting on the blockchain), use `-b <contract file>`

Maian checks for three types of buggy contracts:

1. Suicidal contracts (can be killed by anyone, like the Parity Wallet Library contract), use `-c 0`
2. Prodigal contracts (can send Ether to anyone), use `-c 1`
3. Greedy contracts (nobody can get out Ether), use `-c 2`

For instance, to check if the contract `ParityWalletLibrary.sol` given in Solidity source code with `WalletLibrary` as main contract is suicidal use

	$ python maian.py -s ParityWalletLibrary.sol WalletLibrary -c 0

The output should look like this:

![smiley](maian.png)

To get the full list of options use `python maian.py -h`



### GUI

For GUI inclined audience, we provide  a simple GUI-based Maian. Use `python gui-maian.py` to start it. 
A snapshot of one run is given below

![](./gui-maian.png)

### Automated Fetching

The `fetch_and_check.py` utility can fetch one or more random contracts
directly from a public Ethereum node and run the Maian checks on them. By
default the script connects to Mainnet but other networks can be selected with
the `--network` option. The number of contracts to scan is controlled with
`--count`:

```
$ python fetch_and_check.py --network mainnet --count 5
```

Additional networks can be added to the tool in the future.

### Contract Downloader

The repository also includes a ``contract_downloader.py`` script for gathering
bytecode in bulk. Contracts are stored in ``contracts/contracts.jsonl`` together
with a metadata file describing the covered block range.
The full download logic is explained in
[`docs/data_download_flow.md`](docs/data_download_flow.md).

### SQLite Contract Store

`contract_sqlite_loader.py` can store contract bytecode in a SQLite database.
The tool keeps track of the scanned block range in a `meta` table and enforces
a configurable size limit (40 MB by default).

```
python tool/contract_sqlite_loader.py contracts.db
```

The loader defaults to the AWS Open-Data bucket
`aws-public-blockchain` (`v1.0/eth/contracts/`).
Provide a custom dataset path as the first argument if needed:

```
python tool/contract_sqlite_loader.py /path/to/parquet contracts.db
```

Use `--once` to fetch a single batch instead of running continuously.

### Viewing Database Entries

The `db_head.py` helper prints the first few rows stored in the SQLite database.

```
python tool/db_head.py contracts.db --count 3
```


### Using AWS Open Data

Contract bytecode is also available through the AWS Open-Data program as
Parquet dumps. The ``DataGetterAWSParquet`` helper reads these files from a
local directory or an S3 bucket. Install ``pyarrow`` and point the getter to
the dataset path:

```bash
pip install pyarrow
```

```python
from data_getters import DataGetterAWSParquet

getter = DataGetterAWSParquet("s3://bucket/path")
for page in getter.fetch_chunk(100000, 100100):
    for row in page:
        print(row["Address"], row["BlockNumber"])
```

## Installation

Maian requires Python 3.8 or newer. Install the Python dependencies using:

```bash
pip install web3 z3-solver
```

For the optional GUI you also need `PyQt5`, which can be obtained with your
package manager (for example `sudo apt install python-pyqt5` on Debian based
systems). The tool additionally relies on the following external software:

1. Go Ethereum – <https://ethereum.github.io/go-ethereum/install/>
2. Solidity compiler – <https://docs.soliditylang.org/en/latest/installing-solidity.html>
3. Z3 Theorem prover – <https://github.com/Z3Prover/z3>

### Docker

A `Dockerfile` is provided to build an isolated environment with all
dependencies preinstalled. From the repository root run:

```bash
docker build -t maian .
```

After building, the tool can be executed with:

```bash
docker run --rm maian python tool/maian.py -h
```

### Running Maian

From the repository root you can analyze a contract using:

```bash
python tool/maian.py -s MyContract.sol MyContract -c 0
```

A list of all command line options is available with `python tool/maian.py -h`.

Random contracts can be scanned with `fetch_and_check.py`:

```bash
python tool/fetch_and_check.py --network mainnet --count 5
```

### Running the tests

All tests are executed with `pytest`:

```bash
pytest -q
```

## Supported Operating Systems

Maian should run smoothly on Linux (tested on Ubuntu/Mint) and macOS. Attempts
to run it on Windows have failed.

## Important

To reduce the number of false positives, Maian deploys the analyzed contracts (given either as Solidity or bytecode source) on 
a private blockchain, and confirms the found bugs by sending appropriate transactions to the contracts. 
Therefore, during the execution of the tool, a private Ethereum blockchain is running in the background (blocks are mined on it in the same way as on the Mainnet). Our code stops the private blockchain once Maian finishes the search, however, in some  extreme cases, the blockchain keeps running. Please make sure that after the execution of the program, the private blockchain is off (i.e. `top` does not have `geth` task that corresponds to the private blockchain). 

## License

Maian is released under the [MIT License](https://opensource.org/licenses/MIT), i.e. free for private and commercial use.

 
