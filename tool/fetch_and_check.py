import argparse
import random
import time
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
    code = w3.eth.get_code(address)
    return code.hex()[2:]


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


def scan_multiple_contracts(count: int, network: str = 'mainnet'):
    """Fetch and scan ``count`` random contracts from ``network``."""
    reports = []
    for _ in range(count):
        reports.append(scan_random_contract(network=network))
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
    args = parser.parse_args()

    reports = scan_multiple_contracts(count=args.count, network=args.network)
    for i, rep in enumerate(reports, 1):
        print(f'Scan {i}:')
        for k, v in rep.items():
            print(f'  {k}: {v}')
