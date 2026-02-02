"""Shard iteration order strategies."""

from pisky._pisky import RandomRepeatOrder as _RandomRepeatOrder
from pisky._pisky import SequentialOrder as _SequentialOrder
from pisky.shard.file_shards import FileShards


class Sequential:
    """
    Sequential iteration over shards (in-order, finite).

    Example:
        shards = FileShards.from_pattern("/data", "shard")
        seq = order.Sequential(shards)
        with reader.SequentialConfig(seq) as r:
            for record in r:
                ...
    """

    def __init__(self, shards: FileShards) -> None:
        self._shards = shards
        self._inner = _SequentialOrder(shards._inner)


class RandomRepeat:
    """
    Random repeating iteration over shards (shuffled, infinite).

    Example:
        shards = FileShards.from_pattern("/data", "shard")
        rand = order.RandomRepeat(shards)
        with reader.SequentialConfig(rand) as r:
            for record in r:  # infinite!
                ...

        # With seed for reproducible ordering
        rand = order.RandomRepeat(shards, seed=42)
    """

    def __init__(self, shards: FileShards, seed: int | None = None) -> None:
        self._shards = shards
        self._seed = seed
        self._inner = _RandomRepeatOrder(shards._inner, seed)
