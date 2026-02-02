"""Shard readers."""

from collections.abc import Mapping, Sequence
from types import TracebackType
from typing import Any

from pisky._pisky import RoundRobinReaderConfig as _RoundRobinConfig
from pisky._pisky import SequentialReaderConfig as _SequentialConfig
from pisky._pisky import ShardReader
from pisky.shard.file_shards import FileShards
from pisky.shard.order import RandomRepeat, Sequential
from pisky.tree.named.named_node import NamedNode
from pisky.tree.node import RustNode

# Re-export FileShards so users can do `from pisky.shard import reader; reader.FileShards`
__all__ = ["FileShards", "SequentialConfig", "RoundRobinConfig", "count_records"]


def count_records(shards: FileShards) -> int:
    """
    Count records across all shards.

    Args:
        shards: FileShards to count records from.

    Returns:
        Total number of records across all shards.
    """
    order = Sequential(shards)
    count = 0
    with SequentialConfig(order) as reader:
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

    def __init__(self, order: Sequential | RandomRepeat) -> None:
        self._order = order
        self._config: RustNode | None = None
        self._reader: ShardReader | None = None

    def _make_rust_config(self) -> RustNode:
        """Create the Rust config object."""
        return _SequentialConfig(self._order._inner)

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

    def metadata(self) -> Sequence[Mapping[str, Any]]:
        """Leaf node - return metadata with type and path."""
        return [{"type": "SequentialConfig", "path": str(self._order._shards)}]


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
        max_active: int | None = None,
    ) -> None:
        """
        Args:
            order: The shard order strategy (Sequential or RandomRepeat).
            max_active: Maximum number of shards to keep open simultaneously.
                - None (default): open all shards upfront. Do not use with RandomRepeat.
                - int: keep at most N readers open, replacing exhausted shards on the fly.
        """
        self._order = order
        self._max_active = max_active
        self._config: RustNode | None = None
        self._reader: ShardReader | None = None

    def _make_rust_config(self) -> RustNode:
        """Create the Rust config object."""
        return _RoundRobinConfig(self._order._inner, self._max_active)

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

    def metadata(self) -> Sequence[Mapping[str, Any]]:
        """Leaf node - return metadata with type and path."""
        return [{"type": "RoundRobinConfig", "path": str(self._order._shards)}]
