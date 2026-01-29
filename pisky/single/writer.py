"""Single-file record writer with config-based API."""

from os import PathLike
from pathlib import Path
from typing import Any

from .._pisky import RecordWriter as _RecordWriter
from .._pisky import RecordWriterConfig as _RecordWriterConfig
from ..compression import Compression, Zstd, Uncompressed

PathType = str | Path | PathLike[Any]


class RecordWriter:
    """
    Active record writer for writing records to a file.

    Created by entering a RecordWriterConfig context manager.

    Example:
        with RecordWriterConfig("data.disky") as writer:
            writer.write(b"hello")
            writer.write(b"world")
    """

    def __init__(self, inner: _RecordWriter) -> None:
        self._inner = inner

    def write(self, data: bytes) -> None:
        """
        Write a record to the file.

        Args:
            data: Record bytes to write.
        """
        self._inner.write(data)

    def _close(self) -> None:
        """Close the writer. Called automatically on context exit."""
        self._inner.close()


class RecordWriterConfig:
    """
    Configuration for a single-file record writer.

    This is a config object - it stores parameters but doesn't create any files
    until you enter the context manager.

    Example:
        from pisky import RecordWriterConfig, Zstd, Uncompressed

        # Default (no compression)
        with RecordWriterConfig("data.disky") as writer:
            writer.write(b"hello")

        # With zstd compression
        with RecordWriterConfig("data.disky", compression=Zstd(3)) as writer:
            writer.write(b"hello")

        # Explicit no compression
        with RecordWriterConfig("data.disky", compression=Uncompressed()) as writer:
            writer.write(b"hello")

    Args:
        path: Path to the output file.
        compression: Compression to use. Options:
            - Uncompressed(): No compression (default)
            - Zstd(level): Zstandard compression with given level (1-22, default 3)
    """

    def __init__(
        self,
        path: PathType,
        compression: Compression = Uncompressed(),
    ) -> None:
        self._path = str(path)
        self._compression = compression
        self._config = _RecordWriterConfig(self._path, compression._to_py())
        self._writer: RecordWriter | None = None

    def __enter__(self) -> RecordWriter:
        inner = self._config.__enter__()
        self._writer = RecordWriter(inner)
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
        return False
