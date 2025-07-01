import json
import logging
import os
from pathlib import Path
from typing import Iterable, List, Dict

import requests
from web3 import Web3

DEFAULT_LIMIT_MB = 8
API_URL = "https://api.etherscan.io/api"

logger = logging.getLogger(__name__)


class DataSource:
    """Abstract contract source."""

    def latest_block(self) -> int:
        raise NotImplementedError

    def fetch(self, start_block: int, end_block: int) -> List[Dict]:
        raise NotImplementedError


class EtherscanSource(DataSource):
    """Fetch contracts from the Etherscan API."""

    def __init__(self, api_key: str, verified_only: bool = False) -> None:
        self.api_key = api_key
        self.verified_only = verified_only

    def latest_block(self) -> int:
        params = {
            "module": "proxy",
            "action": "eth_blockNumber",
            "apikey": self.api_key,
        }
        resp = requests.get(API_URL, params=params, timeout=10)
        logger.info("GET %s -> %s", getattr(resp, "url", API_URL), resp.status_code)
        logger.debug("Response length: %d", len(getattr(resp, "content", b"")))
        resp.raise_for_status()
        data = resp.json()
        return int(data["result"], 16)

    def fetch(self, start_block: int, end_block: int) -> List[Dict]:
        params = {
            "module": "contract",
            "action": "getcontractsbytecode",
            "startblock": start_block,
            "endblock": end_block,
            "page": 1,
            "offset": 100,
            "sort": "desc",
            "apikey": self.api_key,
        }
        if self.verified_only:
            params["filter"] = "verified"
        resp = requests.get(API_URL, params=params, timeout=10)
        logger.info("GET %s -> %s", getattr(resp, "url", API_URL), resp.status_code)
        logger.debug("Response length: %d", len(getattr(resp, "content", b"")))
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "1":
            return []
        out = []
        for item in data.get("result", []):
            addr = item.get("ContractAddress") or item.get("address")
            bytecode = item.get("Bytecode") or item.get("bytecode")
            block_val = item.get("BlockNumber") or item.get("block")
            if addr is None or bytecode is None or block_val is None:
                raise ValueError("Malformed response item")
            out.append({"address": addr, "bytecode": bytecode, "block": int(block_val)})
        return out


class RPCSource(DataSource):
    """Fetch contracts by scanning blocks via a Web3 provider."""

    def __init__(self, provider_url: str) -> None:
        self.w3 = Web3(Web3.HTTPProvider(provider_url))
        if not self.w3.is_connected():
            raise RuntimeError("Web3 provider not available")

    def latest_block(self) -> int:
        return self.w3.eth.block_number

    def _gather_candidate_addresses(self, block) -> Iterable[str]:
        for tx in block.transactions:
            to_addr = getattr(tx, "to", None)
            if to_addr:
                yield to_addr
            else:
                receipt = self.w3.eth.get_transaction_receipt(tx.hash)
                if receipt and getattr(receipt, "contractAddress", None):
                    yield receipt.contractAddress

    def fetch(self, start_block: int, end_block: int) -> List[Dict]:
        contracts = []
        logger.info(
            "Scanning blocks %d-%d via %s",
            start_block,
            end_block,
            getattr(getattr(self.w3, "provider", None), "endpoint_uri", "provider"),
        )
        for num in range(start_block, end_block + 1):
            block = self.w3.eth.get_block(num, full_transactions=True)
            for addr in self._gather_candidate_addresses(block):
                code = self.w3.eth.get_code(addr)
                logger.debug("Fetched code for %s (%d bytes)", addr, len(code))
                if code and len(code) > 0:
                    contracts.append(
                        {
                            "address": addr,
                            "bytecode": code.hex()[2:],
                            "block": num,
                        }
                    )
        return contracts


def load_metadata(path: str | Path) -> Dict:
    """Return metadata stored in *path* or defaults."""
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return {
        "oldest_block": None,
        "newest_block": None,
        "size_limit": DEFAULT_LIMIT_MB * 1024 * 1024,
    }


def save_metadata(meta: Dict, path: str | Path) -> None:
    """Write *meta* dictionary to *path*."""
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(meta, fh)


def get_latest_block(api_key: str) -> int:
    """Return the latest block number according to Etherscan."""
    params = {"module": "proxy", "action": "eth_blockNumber", "apikey": api_key}
    resp = requests.get(API_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return int(data["result"], 16)


def fetch_contracts(api_key: str, start_block: int, end_block: int) -> List[Dict]:
    """Return contract info between *start_block* and *end_block*."""
    params = {
        "module": "contract",
        "action": "getcontractsbytecode",
        "startblock": start_block,
        "endblock": end_block,
        "page": 1,
        "offset": 100,
        "sort": "desc",
        "apikey": api_key,
    }
    resp = requests.get(API_URL, params=params, timeout=10)
    logger.info("GET %s -> %s", getattr(resp, "url", API_URL), resp.status_code)
    logger.debug("Response length: %d", len(getattr(resp, "content", b"")))
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") != "1":
        return []
    out = []
    for item in data.get("result", []):
        addr = item.get("ContractAddress") or item.get("address")
        bytecode = item.get("Bytecode") or item.get("bytecode")
        block_val = item.get("BlockNumber") or item.get("block")
        if addr is None or bytecode is None or block_val is None:
            raise ValueError("Malformed response item")
        out.append({"address": addr, "bytecode": bytecode, "block": int(block_val)})
    return out


def _prepend_with_limit(path: Path, lines: List[str], limit: int) -> List[str]:
    existing = []
    if path.exists():
        existing = path.read_text().splitlines(keepends=True)
    all_lines = lines + existing
    size = sum(len(l.encode("utf-8")) for l in all_lines)
    while size > limit and all_lines:
        removed = all_lines.pop()
        size -= len(removed.encode("utf-8"))
    path.write_text("".join(all_lines), encoding="utf-8")
    # Validate JSON integrity after writing to detect truncation
    for ln in all_lines:
        obj = json.loads(ln)
        assert "bytecode" in obj and obj["bytecode"] is not None
    return all_lines


def update_contract_store(
    source: DataSource,
    *,
    contract_file: str = "contracts/contracts.jsonl",
    metadata_file: str = "contracts/metadata.json",
    size_limit_mb: float | None = None,
    start_block: int | None = None,
    end_block: int | None = None,
) -> None:
    """Fetch new contracts and store them prepended in *contract_file*.

    ``source`` defines where contract information is pulled from.
    The function keeps track of the oldest and newest downloaded block in
    ``metadata_file`` and avoids fetching overlapping ranges.
    """
    meta = load_metadata(metadata_file)
    if size_limit_mb is not None:
        meta["size_limit"] = int(size_limit_mb * 1024 * 1024)
    limit = meta.get("size_limit", DEFAULT_LIMIT_MB * 1024 * 1024)
    if start_block is None and end_block is None:
        # When no range is specified and we have no metadata yet, use the
        # current block as the starting point. Otherwise continue from the
        # latest recorded block.
        newest = meta.get("newest_block")
        current = source.latest_block()
        if newest is None:
            start_block = current
            end_block = current
        else:
            end_block = current
            start_block = newest + 1
    elif start_block is None:
        start_block = end_block
    elif end_block is None:
        end_block = start_block

    oldest = meta.get("oldest_block")
    newest = meta.get("newest_block")
    if newest is not None and start_block <= newest <= end_block:
        start_block = newest + 1
    if oldest is not None and start_block <= oldest <= end_block:
        end_block = oldest - 1

    if start_block > end_block:
        save_metadata(meta, metadata_file)
        return

    contracts = source.fetch(start_block, end_block)
    for c in contracts:
        assert c.get("address") and c.get("bytecode") is not None
    if not contracts:
        save_metadata(meta, metadata_file)
        return
    new_lines = [json.dumps(c) + "\n" for c in contracts]
    path = Path(contract_file)
    all_lines = _prepend_with_limit(path, new_lines, limit)
    if all_lines:
        blocks = [json.loads(l)["block"] for l in all_lines]
        meta["newest_block"] = max(blocks)
        meta["oldest_block"] = min(blocks)
    save_metadata(meta, metadata_file)

