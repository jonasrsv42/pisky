"""Shard iteration order strategies."""

from pisky.shard.file_shards import FileShards


class Sequential:
    """
    Sequential iteration over shards (in-order, finite).

    Example:
        shards = FileShards.from_pattern("/data", "shard")
        seq = order.Sequential(shards)
        with reader.Sequential(seq) as r:
            for record in r:
                ...
    """

    def __init__(self, shards: FileShards) -> None:
        self._shards = shards


class RandomRepeat:
    """
    Random repeating iteration over shards (shuffled, infinite).

    Example:
        shards = FileShards.from_pattern("/data", "shard")
        rand = order.RandomRepeat(shards)
        with reader.Sequential(rand) as r:
            for record in r:  # infinite!
                ...
    """

    def __init__(self, shards: FileShards) -> None:
        self._shards = shards
