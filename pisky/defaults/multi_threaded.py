"""
Multi-threaded convenience functions for reading and writing shards.
"""

from contextlib import contextmanager
from pathlib import Path

from pisky.compression import Zstd, Uncompressed
from pisky.protocol import StrPath
from pisky.corruption import CorruptionStrategy
from pisky.shard.file_shards import FileShards as ReaderFileShards
from pisky.multi_threaded.reader import count_records as _count_records
from pisky.shard.order import Sequential, RandomRepeat
from pisky.shard.writer import FileShards as WriterFileShards
from pisky.multi_threaded.reader import MultiThreadedConfig as ReaderConfig
from pisky.multi_threaded.writer import MultiThreadedConfig as WriterConfig


def count_shards_parallel(
    dir_path: StrPath,
    pattern: str = "shard",
) -> int:
    """
    Count total records across all shards in a directory (multi-threaded).

    Args:
        dir_path: Directory containing shard files
        pattern: Glob pattern prefix for shard files (default: "shard")

    Returns:
        Total number of records across all shards

    Example:
        from pisky.defaults import count_shards_parallel

        total = count_shards_parallel("/data/dataset")
        print(f"Found {total} records")
    """
    shards = ReaderFileShards.from_pattern(str(dir_path), pattern)
    return _count_records(shards)


@contextmanager
def read_shards_parallel(
    dir_path: StrPath,
    pattern: str = "shard",
    num_parallel: int = 2,
    worker_threads: int | None = None,
    shuffle: bool = False,
    seed: int | None = None,
    corruption_strategy: CorruptionStrategy = CorruptionStrategy.ERROR,
):
    """
    Read records from sharded files in parallel (multi-threaded).

    Args:
        dir_path: Directory containing shard files
        pattern: Glob pattern prefix for shard files (default: "shard")
        num_parallel: Number of shards to read in parallel (default: 2)
        worker_threads: Number of worker threads (default: auto)
        shuffle: Whether to shuffle shards and repeat infinitely (default: False)
        seed: Random seed for shuffling (required if shuffle=True)
        corruption_strategy: How to handle corrupt records (default: ERROR)

    Yields:
        Iterator over records from all shards

    Example:
        from pisky.defaults import read_shards_parallel

        # Sequential reading
        with read_shards_parallel("/data/dataset", num_parallel=4) as reader:
            for record in reader:
                process(record)

        # Shuffled infinite reading (for training)
        with read_shards_parallel("/data/dataset", shuffle=True, seed=42) as reader:
            for i, record in enumerate(reader):
                if i >= 10000:
                    break
                process(record)
    """
    dir_path_str = str(dir_path)
    shards = ReaderFileShards.from_pattern(dir_path_str, pattern)

    if shuffle:
        if seed is None:
            raise ValueError("seed is required when shuffle=True")
        order = RandomRepeat(shards, seed=seed)
    else:
        order = Sequential(shards)

    with ReaderConfig(
        order,
        num_parallel=num_parallel,
        worker_threads=worker_threads,
        corruption_strategy=corruption_strategy,
    ) as reader:
        yield reader


@contextmanager
def write_shards_parallel(
    dir_path: StrPath,
    pattern: str = "shard",
    num_shards: int = 2,
    worker_threads: int | None = None,
    compression: int | None = 3,
    append: bool = False,
    enable_auto_sharding: bool = True,
):
    """
    Write records to sharded files in parallel (multi-threaded).

    Args:
        dir_path: Directory to write shard files to
        pattern: Prefix for shard file names (default: "shard")
        num_shards: Number of shards to write to concurrently (default: 2)
        worker_threads: Number of worker threads (default: auto)
        compression: Zstd compression level (default: 3, None for no compression)
        append: Start numbering after existing shards instead of overwriting
            (e.g., creates shard_3 if shard_0-2 exist). Default: False
        enable_auto_sharding: Automatically create new shards when current ones
            reach the byte limit, rotating seamlessly (default: True)

    Yields:
        Writer object with write(record) method

    Example:
        from pisky.defaults import write_shards_parallel

        with write_shards_parallel("/data/dataset", num_shards=4) as writer:
            for record in records:
                writer.write(record)
    """
    dir_path_resolved = Path(str(dir_path))
    dir_path_resolved.mkdir(parents=True, exist_ok=True)

    comp = Zstd(compression) if compression is not None else Uncompressed()
    shards = WriterFileShards.from_pattern(str(dir_path_resolved), pattern, append=append)

    with WriterConfig(
        shards,
        num_shards=num_shards,
        worker_threads=worker_threads,
        compression=comp,
        enable_auto_sharding=enable_auto_sharding,
    ) as writer:
        yield writer
