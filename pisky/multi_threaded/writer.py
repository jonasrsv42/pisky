"""Multi-threaded shard writers."""

from typing import Any

from pisky._pisky import MultiThreadedWriterConfig as _MTWriterConfig
from pisky._pisky import MultiThreadedWriter as _MTWriter
from pisky._pisky import MTWriterFileShards as _MTFileShards
from pisky.compression import Compression, Zstd, Uncompressed

__all__ = ["FileShards", "MultiThreadedConfig", "MultiThreadedWriter"]


class MultiThreadedWriter:
    """
    Active multi-threaded shard writer.

    Created by entering a MultiThreadedConfig context manager.
    """

    def __init__(self, inner: _MTWriter) -> None:
        self._inner = inner

    def write(self, data: bytes) -> None:
        """Write a record (distributed across worker threads)."""
        self._inner.write(data)

    def _close(self) -> None:
        """Close the writer. Called automatically on context exit."""
        self._inner.close()


class FileShards:
    """
    A file-based shard factory for multi-threaded writing.

    Creates sequentially numbered files: `{prefix}_0`, `{prefix}_1`, etc.

    Example:
        sink = FileShards.from_pattern("/data", "shard")
        sink = FileShards.from_prefix("/data/shard")
        sink = FileShards.from_pattern("/data", "shard", append=True)
    """

    def __init__(self, inner: _MTFileShards) -> None:
        self._inner = inner

    @staticmethod
    def from_pattern(dir: str, prefix: str, append: bool = False) -> "FileShards":
        """Create by specifying directory and prefix separately."""
        return FileShards(_MTFileShards.from_pattern(dir, prefix, append))

    @staticmethod
    def from_prefix(prefix: str, append: bool = False) -> "FileShards":
        """Create from a path prefix (e.g., '/data/shard' -> dir='/data', prefix='shard')."""
        return FileShards(_MTFileShards.from_prefix(prefix, append))


class MultiThreadedConfig:
    """
    Multi-threaded shard writer - writes to shards in parallel using worker threads.

    Example:
        from pisky.multi_threaded import writer
        from pisky.compression import Zstd

        sink = writer.FileShards.from_pattern("/data", "shard")
        with writer.MultiThreadedConfig(sink, num_shards=4, compression=Zstd(3)) as w:
            w.write(b"hello")
    """

    def __init__(
        self,
        shards: FileShards,
        num_shards: int = 2,
        worker_threads: int | None = None,
        max_bytes_per_writer: int | None = None,
        task_queue_capacity: int | None = None,
        enable_auto_sharding: bool = True,
        compression: Compression = Uncompressed(),
    ) -> None:
        """
        Args:
            shards: The file shards sink.
            num_shards: Number of shards to manage concurrently (default: 2).
            worker_threads: Number of worker threads (default: auto).
            max_bytes_per_writer: Max bytes per writer before rotation (default: auto).
            task_queue_capacity: Size of the task queue (default: auto).
            enable_auto_sharding: Whether to enable auto-sharding (default: True).
            compression: Compression type (Zstd or Uncompressed, default: Uncompressed).
        """
        self._shards = shards
        self._num_shards = num_shards
        self._worker_threads = worker_threads
        self._max_bytes_per_writer = max_bytes_per_writer
        self._task_queue_capacity = task_queue_capacity
        self._enable_auto_sharding = enable_auto_sharding
        self._compression = compression
        self._config: Any = None
        self._writer: Any = None

    def __enter__(self) -> MultiThreadedWriter:
        py_compression = self._compression._to_py()
        self._config = _MTWriterConfig(
            self._shards._inner,
            self._num_shards,
            self._worker_threads,
            self._max_bytes_per_writer,
            self._task_queue_capacity,
            self._enable_auto_sharding,
            py_compression,
        )
        inner = self._config.__enter__()
        self._writer = MultiThreadedWriter(inner)
        return self._writer

    def __exit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        if self._writer is not None:
            self._writer._close()
        self._writer = None
        self._config = None
        return False
