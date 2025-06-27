# Repository Summary

This repository contains **Maian**, a Python based tool for automatically detecting vulnerabilities in Ethereum smart contracts. Maian identifies three classes of bugs:

1. **Suicidal** – contracts that anyone can kill
2. **Prodigal** – contracts that leak Ether to arbitrary users
3. **Greedy** – contracts that lock Ether permanently

The main script `maian.py` analyzes a contract provided either as Solidity source (`-s`), raw bytecode (`-bs`), or compiled bytecode (`-b`). It parses the bytecode, symbolically executes potential transaction sequences using the Z3 solver, and optionally deploys the contract on a private blockchain via `geth` to confirm exploits. A basic GUI front‑end (`gui-maian.py`) is also included, built with PyQt5.

Core functionality resides in the `tool` directory, which contains modules for:

- starting and interacting with a private blockchain (`blockchain.py`)
- parsing EVM bytecode (`parse_code.py`)
- evaluating instructions (`execute_instruction.py` and `execute_block.py`)
- vulnerability checks (`check_suicide.py`, `check_leak.py`, `check_lock.py`)
- contract compilation/deployment (`contracts.py`)

Example contracts and test bytecode are provided in `tool/example_contracts/`. Maian requires Go Ethereum, the Solidity compiler, Z3, and the web3 Python library (plus PyQt5 for the GUI). The project is released under the MIT License.
