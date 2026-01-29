"""File-based shard source."""

from pisky._pisky import ReaderFileShards as _FileShards


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

    @staticmethod
    def from_paths(paths: list[str]) -> "FileShards":
        """Create from an explicit list of file paths."""
        return FileShards(_FileShards.from_paths(paths))

    @staticmethod
    def from_prefix(prefix: str) -> "FileShards":
        """Create by matching all files starting with the given path prefix."""
        return FileShards(_FileShards.from_prefix(prefix))

    @staticmethod
    def from_pattern(dir: str, prefix: str) -> "FileShards":
        """Create by discovering files with the given prefix in a directory."""
        return FileShards(_FileShards.from_pattern(dir, prefix))
