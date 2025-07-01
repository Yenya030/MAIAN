from __future__ import annotations

from typing import Iterable, List, Tuple

class DataGetter:
    """Base class for data getters."""

    def fetch_chunk(self, start_block: int, end_block: int) -> Iterable[List[Tuple[str, str]]]:
        """Return an iterable of pages containing `(address, bytecode)` tuples."""
        raise NotImplementedError
