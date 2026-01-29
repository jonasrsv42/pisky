"""Compression types for the Pisky library."""

from dataclasses import dataclass

from ._pisky import Zstd as _Zstd
from ._pisky import Uncompressed as _Uncompressed


@dataclass(frozen=True)
class Zstd:
    """
    Zstandard compression configuration.

    Args:
        level: Compression level (1-22, default 3).
            Higher levels = better compression but slower.

    Example:
        from pisky import RecordWriterConfig, Zstd

        with RecordWriterConfig("data.disky", compression=Zstd(3)) as writer:
            writer.write(b"hello")
    """

    level: int = 3

    def _to_py(self) -> _Zstd:
        """Get the underlying Rust type."""
        return _Zstd(self.level)


@dataclass(frozen=True)
class Uncompressed:
    """
    No compression configuration.

    Example:
        from pisky import RecordWriterConfig, Uncompressed

        with RecordWriterConfig("data.disky", compression=Uncompressed()) as writer:
            writer.write(b"hello")
    """

    def _to_py(self) -> _Uncompressed:
        """Get the underlying Rust type."""
        return _Uncompressed()


# Type alias for compression options
Compression = Zstd | Uncompressed
