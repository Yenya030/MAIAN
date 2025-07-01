import argparse
import os
import random
import time
from pathlib import Path
from web3 import Web3

from check_suicide import check_one_contract_on_suicide
from check_leak import check_one_contract_on_ether_leak
from check_lock import check_one_contract_on_ether_lock
from values import MyGlobals


NETWORK_PROVIDERS = {
    'mainnet': 'https://rpc.flashbots.net',
    # Additional networks can be added here in the future
    'goerli': 'https://rpc.ankr.com/eth_goerli',
    'sepolia': 'https://rpc.sepolia.org',
}


def get_provider_url(network: str) -> str:
    """Return the provider URL for a given network."""
    try:
        return NETWORK_PROVIDERS[network]
    except KeyError as exc:
        raise ValueError(f'Unknown network: {network}') from exc


def random_contract_address(w3, search_depth=1000, attempts=20):
    latest = w3.eth.block_number
    for _ in range(attempts):
        block_num = latest - random.randint(0, search_depth)
        block = w3.eth.get_block(block_num, full_transactions=True)
        for tx in block.transactions:
            if tx.to:
                code = w3.eth.get_code(tx.to)
                if code and len(code) > 0:
                    return tx.to
    raise RuntimeError('Could not find contract address')


def fetch_contract_bytecode(w3, address):
    """Return bytecode for ``address`` as a hex string without the ``0x`` prefix."""
    code = w3.eth.get_code(address)
    return code.hex()


def run_checks(bytecode, address):
    MyGlobals.max_calldepth_in_normal_search = 2
    results = {}
    start = time.time()
    results['suicidal'] = check_one_contract_on_suicide(
        bytecode, address, False, False, False
    )
    results['suicide_time'] = time.time() - start

    start = time.time()
    results['prodigal'] = check_one_contract_on_ether_leak(
        bytecode, address, False, False, False
    )
    results['prodigal_time'] = time.time() - start

    start = time.time()
    results['greedy'] = check_one_contract_on_ether_lock(
        bytecode, address, False, False
    )
    results['greedy_time'] = time.time() - start

    return results


def scan_random_contract(network: str = 'mainnet'):
    w3 = Web3(Web3.HTTPProvider(get_provider_url(network)))
    if not w3.is_connected():
        raise RuntimeError('Web3 provider not available')

    t0 = time.time()
    address = random_contract_address(w3)
    fetch_time = time.time() - t0

    t1 = time.time()
    code = fetch_contract_bytecode(w3, address)
    code_time = time.time() - t1

    t2 = time.time()
    results = run_checks(code, address)
    check_time = time.time() - t2

    report = {
        'address': address,
        'fetch_time': fetch_time,
        'code_time': code_time,
        'check_time': check_time,
    }
    report.update(results)
    return report


def scan_multiple_contracts(
    count: int,
    network: str = 'mainnet',
    *,
    unique: bool = True,
    address_file: str | None = None,
    report_dir: str = 'reports',
):
    """Fetch and scan ``count`` random contracts from ``network``.

    Parameters
    ----------
    count:
        Number of contracts to scan.
    network:
        Ethereum network to use.
    unique:
        If ``True`` the same contract address will not be scanned twice.
    address_file:
        Optional path to store scanned addresses, one per line.
    report_dir:
        Directory where result files (``suicidal.txt`` etc.) will be stored.
    """
    reports = []
    seen = set()
    addresses = []

    existing = set()
    if unique and address_file and os.path.exists(address_file):
        with open(address_file, 'r', encoding='utf-8') as fh:
            existing = {line.strip() for line in fh if line.strip()}
        seen.update(existing)

    report_path = Path(report_dir)
    report_path.mkdir(parents=True, exist_ok=True)

    def _append(path: Path, address: str) -> None:
        with open(path, 'a', encoding='utf-8') as fh:
            fh.write(address + "\n")

    while len(reports) < count:
        report = scan_random_contract(network=network)
        addr = report.get('address')
        if unique and addr in seen:
            continue
        seen.add(addr)
        addresses.append(addr)
        reports.append(report)

        if report.get('suicidal'):
            _append(report_path / 'suicidal.txt', addr)
        if report.get('prodigal'):
            _append(report_path / 'prodigal.txt', addr)
        if report.get('greedy'):
            _append(report_path / 'greedy.txt', addr)

    if address_file:
        mode = 'a' if os.path.exists(address_file) else 'w'
        with open(address_file, mode, encoding='utf-8') as fh:
            for a in addresses:
                if not unique or a not in existing:
                    fh.write(a + "\n")

    return reports


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Fetch random contracts from a network and run Maian checks'
    )
    parser.add_argument(
        '-n', '--network',
        default='mainnet',
        choices=sorted(NETWORK_PROVIDERS.keys()),
        help='Ethereum network to use (default: mainnet)',
    )
    parser.add_argument(
        '-c', '--count', type=int, default=1,
        help='number of contracts to scan (default: 1)'
    )
    parser.add_argument(
        '--address-file', default='reports/scanned_addresses.txt',
        help='file to store scanned addresses (default: reports/scanned_addresses.txt)'
    )
    parser.add_argument(
        '--report-dir', default='reports',
        help='directory to store result lists (default: reports)'
    )
    parser.add_argument(
        '--allow-duplicates', action='store_true',
        help='allow scanning the same address more than once'
    )
    args = parser.parse_args()

    reports = scan_multiple_contracts(
        count=args.count,
        network=args.network,
        unique=not args.allow_duplicates,
        address_file=args.address_file,
        report_dir=args.report_dir,
    )
    for i, rep in enumerate(reports, 1):
        print(f'Scan {i}:')
        for k, v in rep.items():
            print(f'  {k}: {v}')
