"""File-based shard source."""

from typing import Sequence

from pisky._pisky import ReaderFileShards as _FileShards
from pisky.corruption import CorruptionStrategy
from pisky.protocol import StrPath


class FileShards:
    """
    A collection of file-based shards for reading.

    Example:
        shards = FileShards.from_pattern("/data", "shard")
        shards = FileShards.from_prefix("/data/shard")
        shards = FileShards.from_paths(["/data/shard_0", "/data/shard_1"])

        # With corruption recovery
        shards = FileShards.from_pattern("/data", "shard", corruption_strategy=CorruptionStrategy.RECOVER)
    """

    def __init__(self, inner: _FileShards) -> None:
        self._inner = inner

    def __str__(self) -> str:
        """Return string representation from Rust."""
        return str(self._inner)

    @staticmethod
    def from_paths(
        paths: Sequence[StrPath],
        corruption_strategy: CorruptionStrategy | None = None,
    ) -> "FileShards":
        """Create from an explicit list of file paths."""
        inner = _FileShards.from_paths([str(p) for p in paths])
        if corruption_strategy is not None:
            inner = inner.with_corruption_strategy(corruption_strategy._to_py())
        return FileShards(inner)

    @staticmethod
    def from_prefix(
        prefix: StrPath,
        corruption_strategy: CorruptionStrategy | None = None,
    ) -> "FileShards":
        """Create by matching all files starting with the given path prefix."""
        inner = _FileShards.from_prefix(str(prefix))
        if corruption_strategy is not None:
            inner = inner.with_corruption_strategy(corruption_strategy._to_py())
        return FileShards(inner)

    @staticmethod
    def from_pattern(
        dir: StrPath,
        prefix: str,
        corruption_strategy: CorruptionStrategy | None = None,
    ) -> "FileShards":
        """Create by discovering files with the given prefix in a directory."""
        inner = _FileShards.from_pattern(str(dir), prefix)
        if corruption_strategy is not None:
            inner = inner.with_corruption_strategy(corruption_strategy._to_py())
        return FileShards(inner)
