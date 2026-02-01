"""
Single-threaded convenience functions for reading and writing shards.
"""

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from pisky import RecordReaderConfig
from pisky.protocol import StrPath
from pisky.compression import Zstd, Uncompressed
from pisky.shard.file_shards import FileShards as ReaderFileShards
from pisky.shard.reader import count_records as _count_records
from pisky.shard.writer import FileShards, SequentialConfig


def count_shards(
    dir_path: StrPath,
    pattern: str = "shard",
) -> int:
    """
    Count total records across all shards in a directory.

    Args:
        dir_path: Directory containing shard files
        pattern: Glob pattern prefix for shard files (default: "shard")

    Returns:
        Total number of records across all shards

    Example:
        from pisky.defaults import count_shards

        total = count_shards("/data/dataset")
        print(f"Found {total} records")
    """
    shards = ReaderFileShards.from_pattern(str(dir_path), pattern)
    return _count_records(shards)


@contextmanager
def read_shards(
    dir_path: StrPath,
    pattern: str = "shard",
) -> Iterator:
    """
    Read records from sharded files sequentially (single-threaded).

    Args:
        dir_path: Directory containing shard files
        pattern: Glob pattern prefix for shard files (default: "shard")

    Yields:
        Iterator over records from all shards

    Example:
        from pisky.defaults import read_shards

        with read_shards("/data/dataset") as reader:
            for record in reader:
                process(record)
    """
    dir_path_resolved = Path(str(dir_path))
    shard_files = sorted(dir_path_resolved.glob(f"{pattern}_*"))

    if not shard_files:
        raise ValueError(f"No shard files matching '{pattern}_*' found in {dir_path_resolved}")

    def iterate_shards():
        for shard_file in shard_files:
            with RecordReaderConfig(shard_file) as reader:
                yield from reader

    yield iterate_shards()


@contextmanager
def write_shards(
    dir_path: StrPath,
    pattern: str = "shard",
    max_shard_bytes: int | None = None,
    compression: int | None = 3,
    append: bool = False,
):
    """
    Write records to sharded files (single-threaded).

    Uses SequentialConfig which rotates to new shards when max_shard_bytes is reached.

    Args:
        dir_path: Directory to write shard files to
        pattern: Prefix for shard file names (default: "shard")
        max_shard_bytes: Max bytes per shard before rotating (default: None = 1GB)
        compression: Zstd compression level (default: 3, None for no compression)
        append: Whether to append to existing shards (default: False)

    Yields:
        Writer object with write(record) method

    Example:
        from pisky.defaults import write_shards

        with write_shards("/data/dataset", max_shard_bytes=1_000_000_000) as writer:
            for record in records:
                writer.write(record)
    """
    dir_path_resolved = Path(str(dir_path))
    dir_path_resolved.mkdir(parents=True, exist_ok=True)

    comp = Zstd(compression) if compression is not None else Uncompressed()
    shards = FileShards.from_pattern(str(dir_path_resolved), pattern, append=append)

    with SequentialConfig(shards, compression=comp, max_shard_bytes=max_shard_bytes) as writer:
        yield writer
