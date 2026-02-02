"""Multi-threaded shard readers."""

from collections.abc import Mapping, Sequence
from types import TracebackType
from typing import Any

from pisky._pisky import MultiThreadedReaderConfig as _MTConfig
from pisky._pisky import MultiThreadedReader
from pisky.shard.file_shards import FileShards
from pisky.shard.order import RandomRepeat, Sequential
from pisky.tree.named.named_node import NamedNode
from pisky.tree.node import RustNode

# Re-export FileShards
__all__ = ["FileShards", "MultiThreadedConfig", "count_records"]


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
    with MultiThreadedConfig(order) as reader:
        for _ in reader:
            count += 1
    return count


class MultiThreadedConfig:
    """
    Multi-threaded shard reader - reads shards in parallel using worker threads.

    Example:
        shards = FileShards.from_pattern("/data", "shard")
        seq = order.Sequential(shards)
        with reader.MultiThreadedConfig(seq, num_parallel=4, worker_threads=2) as r:
            for record in r:
                process(bytes(record))
    """

    def __init__(
        self,
        order: Sequential | RandomRepeat,
        num_parallel: int = 2,
        worker_threads: int | None = None,
        queue_size_mb: int | None = None,
    ) -> None:
        """
        Args:
            order: The shard order strategy (Sequential or RandomRepeat).
            num_parallel: Number of shards to read in parallel (default: 2).
            worker_threads: Number of worker threads (default: 2).
            queue_size_mb: Size of the record queue in MB (default: 8).
        """
        self._order = order
        self._num_parallel = num_parallel
        self._worker_threads = worker_threads
        self._queue_size_mb = queue_size_mb
        self._config: RustNode | None = None
        self._reader: MultiThreadedReader | None = None

    def _make_rust_config(self) -> RustNode:
        """Create the Rust config object."""
        return _MTConfig(
            self._order._inner,
            self._num_parallel,
            self._worker_threads,
            self._queue_size_mb,
        )

    def __enter__(self) -> MultiThreadedReader:
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
        metadata = {
            "order": self._order.__class__.__name__,
            "num_parallel": self._num_parallel,
        }
        if self._worker_threads is not None:
            metadata["worker_threads"] = self._worker_threads
        if self._queue_size_mb is not None:
            metadata["queue_size_mb"] = self._queue_size_mb
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
        return [{"type": "MultiThreadedConfig", "path": str(self._order._shards)}]
