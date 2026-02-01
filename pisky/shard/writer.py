"""Shard writers."""

from types import TracebackType

from pisky._pisky import (
    WriterFileShards as _FileShards,
    SequentialWriterConfig as _SeqWriterConfig,
    SequentialWriter as _SeqWriter,
)
from pisky.compression import Compression, Zstd, Uncompressed
from pisky.protocol import StrPath


class FileShards:
    """
    A file-based shard factory for writing.

    Creates sequentially numbered files: `{prefix}_0`, `{prefix}_1`, etc.

    Example:
        sink = FileShards.from_pattern("/data", "shard")
        sink = FileShards.from_prefix("/data/shard")
        sink = FileShards.from_pattern("/data", "shard", append=True)
    """

    def __init__(self, inner: _FileShards) -> None:
        self._inner = inner

    @staticmethod
    def from_pattern(dir: StrPath, prefix: str, append: bool = False) -> "FileShards":
        """Create by specifying directory and prefix separately."""
        return FileShards(_FileShards.from_pattern(str(dir), prefix, append))

    @staticmethod
    def from_prefix(prefix: StrPath, append: bool = False) -> "FileShards":
        """Create from a path prefix (e.g., '/data/shard' -> dir='/data', prefix='shard')."""
        return FileShards(_FileShards.from_prefix(str(prefix), append))


class SequentialWriter:
    """
    Active sequential shard writer.

    Created by entering a SequentialConfig context manager.
    """

    def __init__(self, inner: _SeqWriter) -> None:
        self._inner = inner

    def write(self, data: bytes) -> None:
        """Write a record to the current shard."""
        self._inner.write(data)

    def _close(self) -> None:
        """Close the writer. Called automatically on context exit."""
        self._inner.close()


class SequentialConfig:
    """
    Sequential shard writer - writes to shards, rotating when max_shard_bytes reached.

    Example:
        sink = FileShards.from_pattern("/data", "shard")
        with writer.SequentialConfig(sink, max_shard_bytes=1_000_000_000) as w:
            w.write(b"hello")
    """

    def __init__(
        self,
        shards: FileShards,
        compression: Compression = Uncompressed(),
        max_shard_bytes: int | None = None,
    ) -> None:
        self._shards = shards
        self._compression = compression
        self._max_shard_bytes = max_shard_bytes
        self._config: _SeqWriterConfig | None = None
        self._writer: SequentialWriter | None = None

    def __enter__(self) -> SequentialWriter:
        py_compression = self._compression._to_py()
        self._config = _SeqWriterConfig(
            self._shards._inner,
            py_compression,
            self._max_shard_bytes,
        )
        inner = self._config.__enter__()
        self._writer = SequentialWriter(inner)
        return self._writer

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        if self._writer is not None:
            self._writer._close()
        self._writer = None
        self._config = None
        return False
