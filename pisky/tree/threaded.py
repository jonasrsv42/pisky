"""Threaded node for offloading subtrees to dedicated threads."""

from __future__ import annotations

from collections.abc import Sequence
from types import TracebackType

from pisky._pisky import ThreadedConfig as _ThreadedConfig
from pisky.tree.named.named_node import NamedNode
from pisky.tree.node import NodeConfig, RustNode
from pisky.tree.round_robin import TreeReader


class ThreadedConfig:
    """
    Threaded reader - runs a child node on a dedicated thread.

    This wraps a child node and runs it on a dedicated thread, communicating
    results back via a bounded channel. Useful for parallelizing I/O-bound
    subtrees or decoupling producer from consumer.

    Example:
        from pisky.single import RecordReaderConfig
        from pisky.tree import ThreadedConfig

        config = ThreadedConfig(
            RecordReaderConfig("data.disky"),
            buffer_size=128,
        )
        with config as reader:
            for record in reader:
                # Records are prefetched on a separate thread
                process(bytes(record))

    Nested composition:
        config = ThreadedConfig(
            ShuffleConfig(
                RecordReaderConfig("data.disky"),
                buffer_size=1000,
            ),
            buffer_size=64,
        )
    """

    def __init__(
        self,
        child: NodeConfig,
        buffer_size: int = 64,
    ) -> None:
        """
        Create a threaded config.

        Args:
            child: Child node config to run on a dedicated thread.
            buffer_size: Size of the channel buffer (default: 64).
                Larger values allow more records to be prefetched.
        """
        self._child = child
        self._buffer_size = buffer_size
        self._reader: TreeReader | None = None

    def __enter__(self) -> TreeReader:
        rust_child = self._child.to_rust_node()
        rust_config = _ThreadedConfig(rust_child, self._buffer_size)
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

    def to_rust_node(self) -> RustNode:
        """Convert this config to its Rust equivalent for tree composition."""
        rust_child = self._child.to_rust_node()
        return _ThreadedConfig(rust_child, self._buffer_size)

    @property
    def weight(self) -> float | None:
        """Subtree weight (pass through from child)."""
        return self._child.weight

    def named_children(self) -> Sequence[NamedNode]:
        """Return this node and its subtree."""
        return [
            NamedNode(
                name=self.__class__.__name__,
                weight=self.weight,
                children=self._child.named_children(),
                metadata={"buffer_size": self._buffer_size},
            )
        ]
