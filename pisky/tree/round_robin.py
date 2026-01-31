"""Round-robin reader composition."""

from __future__ import annotations

from types import TracebackType

from pisky._pisky import RoundRobinConfig as _RoundRobinConfig
from pisky._pisky import TreeReader as _TreeReader
from pisky.bytes import Bytes
from pisky.tree.node import NodeConfig, RustNode


class TreeReader:
    """
    Active tree reader - iterates over records from a composed tree.

    Created by entering a tree config context manager (e.g., RoundRobinConfig).
    Supports iteration protocol.

    Example:
        with RoundRobinConfig([...]) as reader:
            for record in reader:
                print(bytes(record))
    """

    def __init__(self, inner: _TreeReader) -> None:
        self._inner = inner

    def __iter__(self) -> TreeReader:
        return self

    def __next__(self) -> Bytes:
        record = self._inner.__next__()
        if record is None:
            raise StopIteration
        return record

    def close(self) -> None:
        """Close the reader, releasing all underlying resources."""
        self._inner.close()


class RoundRobinConfig:
    """
    Round-robin reader composition - interleaves records from multiple sources.

    Takes records from each child in rotation: child0[0], child1[0], child0[1], child1[1], ...
    When a child is exhausted, it's removed from the rotation.

    Children can be any tree node config (RecordReaderConfig, another RoundRobinConfig, etc.).

    Example:
        from pisky.single import RecordReaderConfig
        from pisky.tree import RoundRobinConfig

        config = RoundRobinConfig([
            RecordReaderConfig("a.disky"),
            RecordReaderConfig("b.disky"),
        ])
        with config as reader:
            for record in reader:
                # Records arrive interleaved: a0, b0, a1, b1, ...
                process(bytes(record))

    Nested composition:
        config = RoundRobinConfig([
            RoundRobinConfig([
                RecordReaderConfig("a.disky"),
                RecordReaderConfig("b.disky"),
            ]),
            RecordReaderConfig("c.disky"),
        ])
    """

    def __init__(self, children: list[NodeConfig]) -> None:
        """
        Create a round-robin config.

        Args:
            children: List of child node configs to interleave.
        """
        self._children = children
        self._reader: TreeReader | None = None

    def __enter__(self) -> TreeReader:
        rust_children = [c._to_rust_node() for c in self._children]
        rust_config = _RoundRobinConfig(rust_children)
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
        rust_children = [c._to_rust_node() for c in self._children]
        return _RoundRobinConfig(rust_children)

    @property
    def weight(self) -> float | None:
        """Subtree weight (sum of children weights).

        Raises:
            ValueError: If some children have weights and others don't.
        """
        weights = [c.weight for c in self._children]

        # All None -> None
        if all(w is None for w in weights):
            return None

        # All have weight -> sum
        if all(w is not None for w in weights):
            return sum(weights)  # type: ignore

        # Mixed: error with details
        missing = [
            i for i, w in enumerate(weights) if w is None
        ]
        raise ValueError(
            f"RoundRobinConfig has mixed weights: children at indices {missing} "
            f"are missing weights"
        )
