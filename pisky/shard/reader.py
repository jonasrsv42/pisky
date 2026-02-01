"""Shard readers."""

from collections.abc import Sequence
from types import TracebackType

from pisky._pisky import RoundRobinReaderRandomOrderConfig as _RRRandConfig
from pisky._pisky import RoundRobinReaderSequentialOrderConfig as _RRSeqConfig
from pisky._pisky import SequentialReaderRandomOrderConfig as _SeqRandConfig
from pisky._pisky import SequentialReaderSequentialOrderConfig as _SeqSeqConfig
from pisky._pisky import ShardReader
from pisky.corruption import CorruptionStrategy
from pisky.shard.file_shards import FileShards
from pisky.shard.order import RandomRepeat, Sequential
from pisky.tree.named.named_node import NamedNode
from pisky.tree.node import RustNode

# Re-export FileShards so users can do `from pisky.shard import reader; reader.FileShards`
__all__ = ["FileShards", "SequentialConfig", "RoundRobinConfig", "count_records"]


def count_records(
    shards: FileShards,
    corruption_strategy: CorruptionStrategy = CorruptionStrategy.ERROR,
) -> int:
    """
    Count records across all shards.

    Args:
        shards: FileShards to count records from.
        corruption_strategy: How to handle corrupted data.

    Returns:
        Total number of records across all shards.
    """
    order = Sequential(shards)
    count = 0
    with SequentialConfig(order, corruption_strategy) as reader:
        for _ in reader:
            count += 1
    return count


class SequentialConfig:
    """
    Sequential shard reader - drains each shard before moving to the next.

    Example:
        shards = FileShards.from_pattern("/data", "shard")
        seq = order.Sequential(shards)
        with reader.SequentialConfig(seq) as r:
            for record in r:
                process(bytes(record))
    """

    def __init__(
        self,
        order: Sequential | RandomRepeat,
        corruption_strategy: CorruptionStrategy = CorruptionStrategy.ERROR,
    ) -> None:
        self._order = order
        self._corruption_strategy = corruption_strategy
        self._config: RustNode | None = None
        self._reader: ShardReader | None = None

    def _make_rust_config(self) -> RustNode:
        """Create the Rust config object based on order type."""
        shards = self._order._shards._inner
        py_strategy = self._corruption_strategy._to_py()
        match self._order:
            case Sequential():
                return _SeqSeqConfig(shards, py_strategy)
            case RandomRepeat():
                return _SeqRandConfig(shards, py_strategy, self._order._seed)
            case _:
                raise TypeError(f"Unknown order type: {type(self._order)}")

    def __enter__(self) -> ShardReader:
        self._config = self._make_rust_config()
        self._reader = self._config.__enter__()
        return self._reader

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        if self._reader is not None:
            self._reader.close()
            self._reader = None
        self._config = None
        return False

    def _to_rust_node(self) -> RustNode:
        """Convert this config to its Rust equivalent for tree composition."""
        return self._make_rust_config()

    def serialize(self) -> bytes:
        """Serialize this config to bytes for cross-library transfer."""
        return self._to_rust_node().serialize_as_bytes()

    @property
    def weight(self) -> float | None:
        """Leaf node - no weight."""
        return None

    def named_children(self) -> Sequence[NamedNode]:
        """Return this node as a leaf."""
        metadata = {"order": self._order.__class__.__name__}
        return [
            NamedNode(
                name=self.__class__.__name__,
                weight=self.weight,
                children=[],
                metadata=metadata,
            )
        ]


class RoundRobinConfig:
    """
    Round-robin shard reader - reads one record from each shard in rotation.

    Example:
        shards = FileShards.from_pattern("/data", "shard")
        seq = order.Sequential(shards)
        with reader.RoundRobinConfig(seq, max_active=4) as r:
            for record in r:
                process(bytes(record))
    """

    def __init__(
        self,
        order: Sequential | RandomRepeat,
        corruption_strategy: CorruptionStrategy = CorruptionStrategy.ERROR,
        max_active: int | None = None,
    ) -> None:
        """
        Args:
            order: The shard order strategy (Sequential or RandomRepeat).
            corruption_strategy: How to handle corrupt records (CorruptionStrategy.RECOVER or .ERROR).
            max_active: Maximum number of shards to keep open simultaneously.
                - None (default): open all shards upfront. Do not use with RandomRepeat.
                - int: keep at most N readers open, replacing exhausted shards on the fly.
        """
        self._order = order
        self._corruption_strategy = corruption_strategy
        self._max_active = max_active
        self._config: RustNode | None = None
        self._reader: ShardReader | None = None

    def _make_rust_config(self) -> RustNode:
        """Create the Rust config object based on order type."""
        shards = self._order._shards._inner
        py_strategy = self._corruption_strategy._to_py()
        match self._order:
            case Sequential():
                return _RRSeqConfig(shards, py_strategy, self._max_active)
            case RandomRepeat():
                return _RRRandConfig(
                    shards, py_strategy, self._max_active, self._order._seed
                )
            case _:
                raise TypeError(f"Unknown order type: {type(self._order)}")

    def __enter__(self) -> ShardReader:
        self._config = self._make_rust_config()
        self._reader = self._config.__enter__()
        return self._reader

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        if self._reader is not None:
            self._reader.close()
            self._reader = None
        self._config = None
        return False

    def _to_rust_node(self) -> RustNode:
        """Convert this config to its Rust equivalent for tree composition."""
        return self._make_rust_config()

    def serialize(self) -> bytes:
        """Serialize this config to bytes for cross-library transfer."""
        return self._to_rust_node().serialize_as_bytes()

    @property
    def weight(self) -> float | None:
        """Leaf node - no weight."""
        return None

    def named_children(self) -> Sequence[NamedNode]:
        """Return this node as a leaf."""
        metadata = {"order": self._order.__class__.__name__}
        if self._max_active is not None:
            metadata["max_active"] = self._max_active
        return [
            NamedNode(
                name=self.__class__.__name__,
                weight=self.weight,
                children=[],
                metadata=metadata,
            )
        ]