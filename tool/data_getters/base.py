from __future__ import annotations

from typing import Any, Dict, Iterable, List

class DataGetter:
    """Base class for data getters."""

    def fetch_chunk(self, start_block: int, end_block: int) -> Iterable[List[Dict[str, Any]]]:
        """Yield pages of contract dictionaries.

        Each dictionary contains the ``Address``, ``ByteCode`` and
        ``BlockNumber`` fields.
        """
        raise NotImplementedError
