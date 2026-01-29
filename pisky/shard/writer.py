"""Shard writers."""

from typing import Any

from pisky._pisky import (
    WriterFileShards as _FileShards,
    SequentialWriterConfig as _SeqWriterConfig,
    Zstd,
    Uncompressed,
)

Compression = Zstd | Uncompressed


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
    def from_pattern(dir: str, prefix: str, append: bool = False) -> "FileShards":
        """Create by specifying directory and prefix separately."""
        return FileShards(_FileShards.from_pattern(dir, prefix, append))

    @staticmethod
    def from_prefix(prefix: str, append: bool = False) -> "FileShards":
        """Create from a path prefix (e.g., '/data/shard' -> dir='/data', prefix='shard')."""
        return FileShards(_FileShards.from_prefix(prefix, append))


class Sequential:
    """
    Sequential shard writer - writes to shards, rotating when max_shard_bytes reached.

    Example:
        sink = FileShards.from_pattern("/data", "shard")
        with writer.Sequential(sink, max_shard_bytes=1_000_000_000) as w:
            w.write(b"hello")
    """

    def __init__(
        self,
        shards: FileShards,
        compression: Compression | None = None,
        max_shard_bytes: int | None = None,
    ) -> None:
        self._shards = shards
        self._compression = compression
        self._max_shard_bytes = max_shard_bytes
        self._config: Any = None
        self._writer: Any = None

    def __enter__(self) -> Any:
        self._config = _SeqWriterConfig(
            self._shards._inner,
            self._compression,
            self._max_shard_bytes,
        )
        self._writer = self._config.__enter__()
        return self._writer

    def __exit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        if self._writer is not None:
            self._writer.close()
        self._writer = None
        self._config = None
        return False
