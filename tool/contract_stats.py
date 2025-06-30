from __future__ import annotations

import argparse
import csv
import os
from typing import Iterable, Set

from web3 import Web3

from fetch_and_check import get_provider_url, NETWORK_PROVIDERS


class DataStore:
    """Abstract storage interface for contract statistics."""

    def save(self, stats: dict) -> None:
        raise NotImplementedError


class FileDataStore(DataStore):
    """Persist stats to a CSV file."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._write_header()

    def _write_header(self) -> None:
        if not os.path.exists(self.path):
            with open(self.path, 'w', newline='') as fh:
                writer = csv.writer(fh)
                writer.writerow([
                    'network',
                    'start_block',
                    'end_block',
                    'contract_count',
                    'total_balance_wei',
                    'total_code_size',
                ])

    def save(self, stats: dict) -> None:
        with open(self.path, 'a', newline='') as fh:
            writer = csv.writer(fh)
            writer.writerow([
                stats['network'],
                stats['start_block'],
                stats['end_block'],
                stats['contract_count'],
                stats['total_balance_wei'],
                stats['total_code_size'],
            ])


def _gather_candidate_addresses(block, w3: Web3) -> Iterable[str]:
    """Yield possible contract addresses from transactions in *block*."""
    for tx in block.transactions:
        to_addr = getattr(tx, 'to', None)
        if to_addr:
            yield to_addr
        else:
            receipt = w3.eth.get_transaction_receipt(tx.hash)
            if receipt and receipt.contractAddress:
                yield receipt.contractAddress


def collect_contract_addresses(w3: Web3, start_block: int = 0, end_block: int | None = None) -> Set[str]:
    """Return the set of unique contract addresses between the given blocks."""
    if end_block is None:
        end_block = w3.eth.block_number

    addresses: Set[str] = set()
    for num in range(start_block, end_block + 1):
        block = w3.eth.get_block(num, full_transactions=True)
        for addr in _gather_candidate_addresses(block, w3):
            code = w3.eth.get_code(addr)
            if code and len(code) > 0:
                addresses.add(addr)
    return addresses


def count_contracts(network: str, start_block: int = 0, end_block: int | None = None):
    """Return stats about contracts in the specified block range."""
    w3 = Web3(Web3.HTTPProvider(get_provider_url(network)))
    if not w3.is_connected():
        raise RuntimeError('Web3 provider not available')

    addrs = collect_contract_addresses(w3, start_block, end_block)
    balance = 0
    size = 0
    for a in addrs:
        balance += w3.eth.get_balance(a)
        size += len(w3.eth.get_code(a))

    return {
        'network': network,
        'start_block': start_block,
        'end_block': end_block if end_block is not None else w3.eth.block_number,
        'contract_count': len(addrs),
        'total_balance_wei': balance,
        'total_code_size': size,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description='Count contracts on a network')
    parser.add_argument(
        '-n', '--network',
        default='mainnet',
        choices=sorted(NETWORK_PROVIDERS.keys()),
        help='Ethereum network to use (default: mainnet)',
    )
    parser.add_argument(
        '--start-block', type=int, default=0,
        help='block number to start scanning from (default: 0)'
    )
    parser.add_argument(
        '--end-block', type=int, default=None,
        help='block number to stop scanning at (default: latest)'
    )
    parser.add_argument(
        '--output-file', default='contract_stats.csv',
        help='CSV file to append results to (default: contract_stats.csv)'
    )
    args = parser.parse_args()

    stats = count_contracts(args.network, args.start_block, args.end_block)
    for k, v in stats.items():
        print(f'{k}: {v}')

    store = FileDataStore(args.output_file)
    store.save(stats)


if __name__ == '__main__':
    main()
