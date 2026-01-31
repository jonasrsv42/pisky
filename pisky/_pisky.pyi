"""Type stubs for the pisky native module."""

from types import TracebackType
from typing import Any, Iterator
from pathlib import Path
from os import PathLike

from .bytes import Bytes

# Type alias for path-like objects
PathType = str | Path | PathLike[Any]


# =============================================================================
# Logging
# =============================================================================

def set_log_level(level_str: str) -> None:
    """
    Set the logging level for the Disky library.

    Args:
        level_str: One of "trace", "debug", "info", "warn", "error", or "off"

    Raises:
        IOError: If an invalid log level is provided
    """
    ...


# =============================================================================
# Corruption strategy
# =============================================================================

class PyCorruptionStrategy:
    """Enum for corruption handling strategies."""

    Error: "PyCorruptionStrategy"
    Recover: "PyCorruptionStrategy"


# =============================================================================
# Compression
# =============================================================================

class Zstd:
    """Zstandard compression configuration."""

    def __init__(self, level: int = 3) -> None:
        """
        Create a Zstd compression config.

        Args:
            level: Compression level (1-22, default 3). Higher = better compression but slower.
        """
        ...


class Uncompressed:
    """No compression configuration."""

    def __init__(self) -> None:
        """Create an uncompressed config."""
        ...


# =============================================================================
# Single file API
# =============================================================================

class RecordWriterConfig:
    """Configuration for a single-file record writer."""

    def __init__(
        self,
        path: PathType,
        compression: Zstd | Uncompressed | None = None,
    ) -> None:
        """
        Create a writer config.

        Args:
            path: Path to the output file.
            compression: Compression to use (Zstd or Uncompressed). Default: no compression.
        """
        ...

    def __enter__(self) -> "RecordWriter": ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool: ...


class RecordWriter:
    """Active record writer for writing records to a file."""

    def write(self, data: bytes) -> None:
        """Write a record to the file."""
        ...


class RecordReaderConfig:
    """Configuration for a single-file record reader."""

    def __init__(
        self,
        path: PathType,
        corruption_strategy: PyCorruptionStrategy | None = None,
    ) -> None:
        """
        Create a reader config.

        Args:
            path: Path to the disky file.
            corruption_strategy: PyCorruptionStrategy.Recover to skip corrupted chunks,
                or None/PyCorruptionStrategy.Error to raise on corruption.
        """
        ...

    def __enter__(self) -> "RecordReader": ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool: ...


class RecordReader(Iterator[Bytes]):
    """Active record reader for iterating over records."""

    def read(self) -> Bytes | None:
        """Read the next record, or None if EOF."""
        ...

    def close(self) -> None:
        """Close the reader, releasing the underlying file handle."""
        ...

    def __iter__(self) -> "RecordReader": ...
    def __next__(self) -> Bytes: ...


# =============================================================================
# Shard source (shared by shard and multi-threaded readers)
# =============================================================================

class ReaderFileShards:
    """File-based shard source for readers."""

    @staticmethod
    def from_pattern(dir: str, prefix: str) -> "ReaderFileShards":
        """Create from directory and prefix pattern."""
        ...

    @staticmethod
    def from_prefix(prefix: str) -> "ReaderFileShards":
        """Create from a path prefix."""
        ...

    @staticmethod
    def from_paths(paths: list[str]) -> "ReaderFileShards":
        """Create from explicit list of paths."""
        ...


# =============================================================================
# Shard readers (shared reader type for all configs)
# =============================================================================

class ShardReader(Iterator[Bytes]):
    """Active shard reader - shared type for all shard reader configs."""

    def close(self) -> None:
        """Close the reader, releasing all underlying file handles."""
        ...

    def __iter__(self) -> "ShardReader": ...
    def __next__(self) -> Bytes: ...


class SequentialReaderSequentialOrderConfig:
    """Sequential shard reader with sequential order."""

    def __init__(
        self,
        shards: ReaderFileShards,
        corruption_strategy: PyCorruptionStrategy | None = None,
    ) -> None: ...

    def __enter__(self) -> ShardReader: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool: ...


class SequentialReaderRandomOrderConfig:
    """Sequential shard reader with random repeating order."""

    def __init__(
        self,
        shards: ReaderFileShards,
        corruption_strategy: PyCorruptionStrategy | None = None,
    ) -> None: ...

    def __enter__(self) -> ShardReader: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool: ...


class RoundRobinReaderSequentialOrderConfig:
    """Round-robin shard reader with sequential order."""

    def __init__(
        self,
        shards: ReaderFileShards,
        corruption_strategy: PyCorruptionStrategy | None = None,
        max_active: int | None = None,
    ) -> None: ...

    def __enter__(self) -> ShardReader: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool: ...


class RoundRobinReaderRandomOrderConfig:
    """Round-robin shard reader with random repeating order."""

    def __init__(
        self,
        shards: ReaderFileShards,
        corruption_strategy: PyCorruptionStrategy | None = None,
        max_active: int | None = None,
    ) -> None: ...

    def __enter__(self) -> ShardReader: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool: ...


# =============================================================================
# Shard writers
# =============================================================================

class WriterFileShards:
    """File-based shard sink for writers."""

    @staticmethod
    def from_pattern(dir: str, prefix: str, append: bool = False) -> "WriterFileShards":
        """Create from directory and prefix pattern."""
        ...

    @staticmethod
    def from_prefix(prefix: str, append: bool = False) -> "WriterFileShards":
        """Create from a path prefix."""
        ...


class SequentialWriterConfig:
    """Configuration for sequential shard writer."""

    def __init__(
        self,
        shards: WriterFileShards,
        compression: Zstd | Uncompressed | None = None,
        max_shard_bytes: int | None = None,
    ) -> None: ...

    def __enter__(self) -> "SequentialWriter": ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool: ...


class SequentialWriter:
    """Active sequential shard writer."""

    def write(self, data: bytes) -> None:
        """Write a record."""
        ...


# =============================================================================
# Multi-threaded readers (shared reader type for all configs)
# =============================================================================

class MultiThreadedReader(Iterator[Bytes]):
    """Active multi-threaded reader - shared type for all multi-threaded configs."""

    def close(self) -> None: ...
    def __iter__(self) -> "MultiThreadedReader": ...
    def __next__(self) -> Bytes: ...


class MultiThreadedReaderSequentialOrderConfig:
    """Multi-threaded reader with sequential order."""

    def __init__(
        self,
        shards: ReaderFileShards,
        num_parallel: int = 2,
        worker_threads: int | None = None,
        queue_size_mb: int | None = None,
        corruption_strategy: PyCorruptionStrategy | None = None,
    ) -> None: ...

    def __enter__(self) -> MultiThreadedReader: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool: ...


class MultiThreadedReaderRandomOrderConfig:
    """Multi-threaded reader with random repeating order."""

    def __init__(
        self,
        shards: ReaderFileShards,
        num_parallel: int = 2,
        worker_threads: int | None = None,
        queue_size_mb: int | None = None,
        corruption_strategy: PyCorruptionStrategy | None = None,
    ) -> None: ...

    def __enter__(self) -> MultiThreadedReader: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool: ...


# =============================================================================
# Multi-threaded writers (uses WriterFileShards from shard writers)
# =============================================================================

class MultiThreadedWriterConfig:
    """Configuration for multi-threaded shard writer."""

    def __init__(
        self,
        shards: WriterFileShards,
        num_shards: int = 2,
        worker_threads: int | None = None,
        max_bytes_per_writer: int | None = None,
        task_queue_capacity: int | None = None,
        enable_auto_sharding: bool = True,
        compression: Zstd | Uncompressed | None = None,
    ) -> None: ...

    def __enter__(self) -> "MultiThreadedWriter": ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool: ...


class MultiThreadedWriter:
    """Active multi-threaded shard writer."""

    def write(self, data: bytes) -> None:
        """Write a record."""
        ...


# =============================================================================
# Tree-based composition (internal Rust types)
# =============================================================================

class RoundRobinConfig:
    """Internal Rust round-robin config."""

    def __init__(self, children: list[Any]) -> None: ...
    def __enter__(self) -> "TreeReader": ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool: ...


class ShuffleConfig:
    """Internal Rust shuffle config."""

    def __init__(
        self,
        child: Any,
        buffer_size: int = 1000,
        seed: int | None = None,
    ) -> None: ...
    def __enter__(self) -> "TreeReader": ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool: ...


class ThreadedConfig:
    """Internal Rust threaded config."""

    def __init__(
        self,
        child: Any,
        buffer_size: int = 64,
    ) -> None: ...
    def __enter__(self) -> "TreeReader": ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool: ...


class SamplingConfig:
    """Internal Rust sampling config."""

    def __init__(
        self,
        sources: list[tuple[Any, float]],
        seed: int | None = None,
    ) -> None: ...
    def __enter__(self) -> "TreeReader": ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool: ...


class TreeReader(Iterator[Bytes]):
    """Internal Rust tree reader."""

    def close(self) -> None:
        """Close the reader, releasing all underlying resources."""
        ...

    def __iter__(self) -> "TreeReader": ...
    def __next__(self) -> Bytes | None: ...
