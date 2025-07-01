import json
import logging
import os
from pathlib import Path
from typing import Iterable, List, Dict

from data_getters import DataGetterAWSParquet

DEFAULT_LIMIT_MB = 8

logger = logging.getLogger(__name__)


class DataSource:
    """Abstract contract source."""

    def latest_block(self) -> int:
        raise NotImplementedError

    def fetch(self, start_block: int, end_block: int) -> List[Dict]:
        raise NotImplementedError




class ParquetSource(DataSource):
    """Fetch contracts from a Parquet dataset (local or S3)."""

    def __init__(self, path: str) -> None:
        self._getter = DataGetterAWSParquet(path)

    def latest_block(self) -> int:
        table = self._getter._dataset.to_table(columns=["block_number"])
        return max(table["block_number"].to_pylist())

    def fetch(self, start_block: int, end_block: int) -> List[Dict]:
        contracts = []
        for page in self._getter.fetch_chunk(start_block, end_block):
            for row in page:
                contracts.append({
                    "address": row["Address"],
                    "bytecode": row["ByteCode"],
                    "block": row["BlockNumber"],
                })
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

