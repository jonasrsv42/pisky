"""LazyWeightedNodeConfig for deferred weight and child resolution."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from types import TracebackType

from pisky.bytes import Bytes
from pisky.tree.named.named_node import NamedNode
from pisky.tree.node import NodeConfig, RustNode


# Type for lazy factory
LazyWeightedNodeConfigFactory = Callable[[], tuple[NodeConfig, float]]


class LazyWeightedNodeConfig:
    """Lazily attaches a weight to a node config.

    The factory is called on first access to weight or to_rust_node().
    Useful when path or weight is computed at runtime.

    Example:
        def make_config():
            path = discover_latest_checkpoint("/data/en")
            weight = get_dataset_importance("en")
            return (RecordReaderConfig(path), weight)

        LazyWeightedNodeConfig(make_config)
    """

    def __init__(self, factory: LazyWeightedNodeConfigFactory) -> None:
        self._factory: LazyWeightedNodeConfigFactory | None = factory
        self._child: NodeConfig | None = None
        self._weight: float = 0.0
        self._resolved = False

    def _resolve(self) -> None:
        """Resolve lazy factory if not already resolved."""
        if self._resolved:
            return
        if self._factory is None:
            raise RuntimeError("LazyWeightedNodeConfig has no factory to resolve")
        child, weight = self._factory()
        if weight <= 0:
            raise ValueError("weight must be positive")
        self._child = child
        self._weight = weight
        self._factory = None
        self._resolved = True

    def to_rust_node(self) -> RustNode:
        """Delegate to wrapped child (transparent in Rust tree)."""
        self._resolve()
        assert self._child is not None
        return self._child.to_rust_node()

    def __enter__(self) -> Iterator[Bytes]:
        """Delegate to child."""
        self._resolve()
        assert self._child is not None
        return self._child.__enter__()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        """Delegate to child."""
        assert self._child is not None
        return self._child.__exit__(exc_type, exc_val, exc_tb)

    @property
    def weight(self) -> float:
        """Subtree weight (this weight + child's weight, additive)."""
        self._resolve()
        assert self._child is not None
        child_weight = self._child.weight
        if child_weight is None:
            return self._weight
        return self._weight + child_weight

    def named_children(self) -> Sequence[NamedNode]:
        """Return this node and its subtree."""
        self._resolve()
        assert self._child is not None
        return [
            NamedNode(
                name=self.__class__.__name__,
                weight=self.weight,
                children=self._child.named_children(),
                metadata={"weight": self._weight},
            )
        ]
