"""Shard readers."""

from typing import Any

from pisky._pisky import (
    RoundRobinReaderRandomOrderConfig as _RRRandConfig,
    RoundRobinReaderSequentialOrderConfig as _RRSeqConfig,
    SequentialReaderRandomOrderConfig as _SeqRandConfig,
    SequentialReaderSequentialOrderConfig as _SeqSeqConfig,
)
from pisky.shard.file_shards import FileShards
from pisky.shard.order import RandomRepeat, Sequential as SeqOrder

# Re-export FileShards so users can do `from pisky.shard import reader; reader.FileShards`
__all__ = ["FileShards", "Sequential", "RoundRobin"]


class Sequential:
    """
    Sequential shard reader - drains each shard before moving to the next.

    Example:
        shards = FileShards.from_pattern("/data", "shard")
        seq = order.Sequential(shards)
        with reader.Sequential(seq) as r:
            for record in r:
                process(bytes(record))
    """

    def __init__(
        self,
        order: SeqOrder | RandomRepeat,
        corruption_strategy: str | None = None,
    ) -> None:
        self._order = order
        self._corruption_strategy = corruption_strategy
        self._config: Any = None
        self._reader: Any = None

    def __enter__(self) -> Any:
        shards = self._order._shards._inner
        if isinstance(self._order, SeqOrder):
            self._config = _SeqSeqConfig(shards, self._corruption_strategy)
        else:
            self._config = _SeqRandConfig(shards, self._corruption_strategy)
        self._reader = self._config.__enter__()
        return self._reader

    def __exit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        self._reader = None
        self._config = None
        return False


class RoundRobin:
    """
    Round-robin shard reader - reads one record from each shard in rotation.

    Example:
        shards = FileShards.from_pattern("/data", "shard")
        seq = order.Sequential(shards)
        with reader.RoundRobin(seq, max_active=4) as r:
            for record in r:
                process(bytes(record))
    """

    def __init__(
        self,
        order: SeqOrder | RandomRepeat,
        corruption_strategy: str | None = None,
        max_active: int | None = None,
    ) -> None:
        """
        Args:
            order: The shard order strategy (Sequential or RandomRepeat).
            corruption_strategy: How to handle corrupt records ("recover" or "error").
            max_active: Maximum number of shards to keep open simultaneously.
                - None (default): open all shards upfront. Do not use with RandomRepeat.
                - int: keep at most N readers open, replacing exhausted shards on the fly.
        """
        self._order = order
        self._corruption_strategy = corruption_strategy
        self._max_active = max_active
        self._config: Any = None
        self._reader: Any = None

    def __enter__(self) -> Any:
        shards = self._order._shards._inner
        if isinstance(self._order, SeqOrder):
            self._config = _RRSeqConfig(shards, self._corruption_strategy, self._max_active)
        else:
            self._config = _RRRandConfig(shards, self._corruption_strategy, self._max_active)
        self._reader = self._config.__enter__()
        return self._reader

    def __exit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        self._reader = None
        self._config = None
        return False
