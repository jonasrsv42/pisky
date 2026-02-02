"""File-based shard source."""

from typing import Sequence

from pisky._pisky import ReaderFileShards as _FileShards
from pisky.protocol import StrPath


class FileShards:
    """
    A collection of file-based shards for reading.

    Example:
        shards = FileShards.from_pattern("/data", "shard")
        shards = FileShards.from_prefix("/data/shard")
        shards = FileShards.from_paths(["/data/shard_0", "/data/shard_1"])
    """

    def __init__(self, inner: _FileShards) -> None:
        self._inner = inner

    def __str__(self) -> str:
        """Return string representation from Rust."""
        return str(self._inner)

    @staticmethod
    def from_paths(paths: Sequence[StrPath]) -> "FileShards":
        """Create from an explicit list of file paths."""
        return FileShards(_FileShards.from_paths([str(p) for p in paths]))

    @staticmethod
    def from_prefix(prefix: StrPath) -> "FileShards":
        """Create by matching all files starting with the given path prefix."""
        return FileShards(_FileShards.from_prefix(str(prefix)))

    @staticmethod
    def from_pattern(dir: StrPath, prefix: str) -> "FileShards":
        """Create by discovering files with the given prefix in a directory."""
        return FileShards(_FileShards.from_pattern(str(dir), prefix))
