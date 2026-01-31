"""Reservoir shuffle for streaming data randomization."""

from __future__ import annotations

from types import TracebackType

from pisky._pisky import ShuffleConfig as _ShuffleConfig
from pisky.tree.node import NodeConfig, RustNode
from pisky.tree.round_robin import TreeReader


class ShuffleConfig:
    """
    Shuffle reader - randomizes record order using reservoir sampling.

    Uses a buffer to shuffle records from the child source. Larger buffer
    sizes provide better randomization but use more memory.

    Example:
        from pisky.single import RecordReaderConfig
        from pisky.tree import ShuffleConfig

        config = ShuffleConfig(
            RecordReaderConfig("data.disky"),
            buffer_size=1000,
            seed=42,
        )
        with config as reader:
            for record in reader:
                # Records arrive in shuffled order
                process(bytes(record))

    Nested composition:
        config = ShuffleConfig(
            RoundRobinConfig([
                RecordReaderConfig("a.disky"),
                RecordReaderConfig("b.disky"),
            ]),
            buffer_size=500,
        )
    """

    def __init__(
        self,
        child: NodeConfig,
        buffer_size: int = 1000,
        seed: int | None = None,
    ) -> None:
        """
        Create a shuffle config.

        Args:
            child: Child node config to shuffle.
            buffer_size: Size of the shuffle buffer (default: 1000).
                Larger values provide better randomization.
            seed: Optional random seed for deterministic shuffling.
        """
        self._child = child
        self._buffer_size = buffer_size
        self._seed = seed
        self._reader: TreeReader | None = None

    def __enter__(self) -> TreeReader:
        rust_child = self._child._to_rust_node()
        rust_config = _ShuffleConfig(rust_child, self._buffer_size, self._seed)
        inner = rust_config.__enter__()
        self._reader = TreeReader(inner)
        return self._reader

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        if self._reader is not None:
            self._reader.close()
            self._reader = None
        return False

    def _to_rust_node(self) -> RustNode:
        """Convert this config to its Rust equivalent for tree composition."""
        rust_child = self._child._to_rust_node()
        return _ShuffleConfig(rust_child, self._buffer_size, self._seed)

    @property
    def weight(self) -> float | None:
        """Subtree weight (pass through from child)."""
        return self._child.weight
