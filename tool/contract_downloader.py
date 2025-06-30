import json
import os
from pathlib import Path
from typing import List, Dict

import requests

DEFAULT_LIMIT_MB = 8
API_URL = "https://api.etherscan.io/api"


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
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") != "1":
        return []
    out = []
    for item in data.get("result", []):
        out.append(
            {
                "address": item.get("ContractAddress") or item.get("address"),
                "bytecode": item.get("Bytecode") or item.get("bytecode"),
                "block": int(item.get("BlockNumber") or item.get("block")),
            }
        )
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
    return all_lines


def update_contract_store(
    api_key: str,
    *,
    contract_file: str = "contracts/contracts.jsonl",
    metadata_file: str = "contracts/metadata.json",
    size_limit_mb: float | None = None,
    start_block: int | None = None,
    end_block: int | None = None,
) -> None:
    """Fetch new contracts and store them prepended in *contract_file*."""
    meta = load_metadata(metadata_file)
    if size_limit_mb is not None:
        meta["size_limit"] = int(size_limit_mb * 1024 * 1024)
    limit = meta.get("size_limit", DEFAULT_LIMIT_MB * 1024 * 1024)
    if start_block is None:
        start_block = (meta.get("newest_block") or 0) + 1
    if end_block is None:
        end_block = get_latest_block(api_key)
    contracts = fetch_contracts(api_key, start_block, end_block)
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
