"""Type stubs for the pisky native module."""

from typing import Any, Iterator
from pathlib import Path
from os import PathLike

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
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: Any,
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
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool: ...


class RecordReader(Iterator[bytes]):
    """Active record reader for iterating over records."""

    def read(self) -> bytes | None:
        """Read the next record, or None if EOF."""
        ...

    def __iter__(self) -> "RecordReader": ...
    def __next__(self) -> bytes: ...


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
# Shard readers - Sequential drain
# =============================================================================

class SequentialReaderSequentialOrderConfig:
    """Sequential shard reader with sequential order."""

    def __init__(
        self,
        shards: ReaderFileShards,
        corruption_strategy: PyCorruptionStrategy | None = None,
    ) -> None: ...

    def __enter__(self) -> "SequentialReaderSequentialOrder": ...
    def __exit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool: ...


class SequentialReaderSequentialOrder(Iterator[bytes]):
    """Active sequential reader with sequential order."""

    def __iter__(self) -> "SequentialReaderSequentialOrder": ...
    def __next__(self) -> bytes: ...


class SequentialReaderRandomOrderConfig:
    """Sequential shard reader with random repeating order."""

    def __init__(
        self,
        shards: ReaderFileShards,
        corruption_strategy: PyCorruptionStrategy | None = None,
    ) -> None: ...

    def __enter__(self) -> "SequentialReaderRandomOrder": ...
    def __exit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool: ...


class SequentialReaderRandomOrder(Iterator[bytes]):
    """Active sequential reader with random order (infinite)."""

    def __iter__(self) -> "SequentialReaderRandomOrder": ...
    def __next__(self) -> bytes: ...


# =============================================================================
# Shard readers - Round robin
# =============================================================================

class RoundRobinReaderSequentialOrderConfig:
    """Round-robin shard reader with sequential order."""

    def __init__(
        self,
        shards: ReaderFileShards,
        corruption_strategy: PyCorruptionStrategy | None = None,
        max_active: int | None = None,
    ) -> None: ...

    def __enter__(self) -> "RoundRobinReaderSequentialOrder": ...
    def __exit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool: ...


class RoundRobinReaderSequentialOrder(Iterator[bytes]):
    """Active round-robin reader with sequential order."""

    def __iter__(self) -> "RoundRobinReaderSequentialOrder": ...
    def __next__(self) -> bytes: ...


class RoundRobinReaderRandomOrderConfig:
    """Round-robin shard reader with random repeating order."""

    def __init__(
        self,
        shards: ReaderFileShards,
        corruption_strategy: PyCorruptionStrategy | None = None,
        max_active: int | None = None,
    ) -> None: ...

    def __enter__(self) -> "RoundRobinReaderRandomOrder": ...
    def __exit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool: ...


class RoundRobinReaderRandomOrder(Iterator[bytes]):
    """Active round-robin reader with random order (infinite)."""

    def __iter__(self) -> "RoundRobinReaderRandomOrder": ...
    def __next__(self) -> bytes: ...


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
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool: ...


class SequentialWriter:
    """Active sequential shard writer."""

    def write(self, data: bytes) -> None:
        """Write a record."""
        ...


# =============================================================================
# Multi-threaded readers
# =============================================================================

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

    def __enter__(self) -> "MultiThreadedReaderSequentialOrder": ...
    def __exit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool: ...


class MultiThreadedReaderSequentialOrder(Iterator[bytes]):
    """Active multi-threaded reader with sequential order."""

    def close(self) -> None: ...
    def queued_records(self) -> int: ...
    def queued_bytes(self) -> int: ...
    def __iter__(self) -> "MultiThreadedReaderSequentialOrder": ...
    def __next__(self) -> bytes: ...


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

    def __enter__(self) -> "MultiThreadedReaderRandomOrder": ...
    def __exit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool: ...


class MultiThreadedReaderRandomOrder(Iterator[bytes]):
    """Active multi-threaded reader with random order (infinite)."""

    def close(self) -> None: ...
    def queued_records(self) -> int: ...
    def queued_bytes(self) -> int: ...
    def __iter__(self) -> "MultiThreadedReaderRandomOrder": ...
    def __next__(self) -> bytes: ...


# =============================================================================
# Multi-threaded writers
# =============================================================================

class MTWriterFileShards:
    """File-based shard sink for multi-threaded writers."""

    @staticmethod
    def from_pattern(dir: str, prefix: str, append: bool = False) -> "MTWriterFileShards":
        """Create from directory and prefix pattern."""
        ...

    @staticmethod
    def from_prefix(prefix: str, append: bool = False) -> "MTWriterFileShards":
        """Create from a path prefix."""
        ...


class MultiThreadedWriterConfig:
    """Configuration for multi-threaded shard writer."""

    def __init__(
        self,
        shards: MTWriterFileShards,
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
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool: ...


class MultiThreadedWriter:
    """Active multi-threaded shard writer."""

    def write(self, data: bytes) -> None:
        """Write a record."""
        ...
