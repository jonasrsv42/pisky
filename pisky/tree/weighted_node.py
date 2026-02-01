"""WeightedNodeConfig for attaching weights to tree nodes."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from types import TracebackType

from pisky.bytes import Bytes
from pisky.tree.named.named_node import NamedNode
from pisky.tree.node import NodeConfig, RustNode


class WeightedNodeConfig:
    """Attaches a weight to any node config.

    WeightedNodeConfig is additive - stacked WeightedNodeConfigs sum their weights:
        WeightedNodeConfig(WeightedNodeConfig(x, 2.0), 3.0).weight == 5.0

    Example:
        WeightedNodeConfig(RecordReaderConfig("data.disky"), weight=2.0)
    """

    def __init__(self, child: NodeConfig, weight: float) -> None:
        if weight is None:
            raise ValueError("weight is required")
        if weight <= 0:
            raise ValueError("weight must be positive")
        self._child = child
        self._weight = weight

    def _to_rust_node(self) -> RustNode:
        """Delegate to wrapped child (transparent in Rust tree)."""
        return self._child._to_rust_node()

    def serialize(self) -> bytes:
        """Serialize this config to bytes for cross-library transfer."""
        return self._to_rust_node().serialize_as_bytes()

    def __enter__(self) -> Iterator[Bytes]:
        """Delegate to child."""
        return self._child.__enter__()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        """Delegate to child."""
        return self._child.__exit__(exc_type, exc_val, exc_tb)

    @property
    def weight(self) -> float:
        """Subtree weight (this weight + child's weight, additive)."""
        child_weight = self._child.weight
        if child_weight is None:
            return self._weight
        return self._weight + child_weight

    def named_children(self) -> Sequence[NamedNode]:
        """Return this node and its subtree."""
        return [
            NamedNode(
                name=self.__class__.__name__,
                weight=self.weight,
                children=self._child.named_children(),
                metadata={"weight": self._weight},
            )
        ]
