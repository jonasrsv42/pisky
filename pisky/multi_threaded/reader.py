"""Multi-threaded shard readers."""

from typing import Any

from pisky._pisky import MultiThreadedReaderRandomOrderConfig as _MTRandConfig
from pisky._pisky import MultiThreadedReaderSequentialOrderConfig as _MTSeqConfig
from pisky.corruption import CorruptionStrategy
from pisky.shard.file_shards import FileShards
from pisky.shard.order import RandomRepeat, Sequential

# Re-export FileShards
__all__ = ["FileShards", "MultiThreadedConfig", "count_records"]


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
    with MultiThreadedConfig(order, corruption_strategy=corruption_strategy) as reader:
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
        corruption_strategy: CorruptionStrategy = CorruptionStrategy.ERROR,
    ) -> None:
        """
        Args:
            order: The shard order strategy (Sequential or RandomRepeat).
            num_parallel: Number of shards to read in parallel (default: 2).
            worker_threads: Number of worker threads (default: auto).
            queue_size_mb: Size of the record queue in MB (default: auto).
            corruption_strategy: How to handle corrupt records (CorruptionStrategy.RECOVER or .ERROR).
        """
        self._order = order
        self._num_parallel = num_parallel
        self._worker_threads = worker_threads
        self._queue_size_mb = queue_size_mb
        self._corruption_strategy = corruption_strategy
        self._config: Any = None
        self._reader: Any = None

    def __enter__(self) -> Any:
        shards = self._order._shards._inner
        py_strategy = self._corruption_strategy._to_py()
        match self._order:
            case Sequential():
                self._config = _MTSeqConfig(
                    shards,
                    self._num_parallel,
                    self._worker_threads,
                    self._queue_size_mb,
                    py_strategy,
                )
            case RandomRepeat():
                self._config = _MTRandConfig(
                    shards,
                    self._num_parallel,
                    self._worker_threads,
                    self._queue_size_mb,
                    py_strategy,
                    self._order._seed,
                )
            case _:
                raise TypeError(f"Unknown order type: {type(self._order)}")
        self._reader = self._config.__enter__()
        return self._reader

    def __exit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        if self._reader is not None:
            self._reader.close()
        self._reader = None
        self._config = None
        return False
