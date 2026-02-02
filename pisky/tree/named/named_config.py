"""NamedNodeConfig for attaching names to tree nodes."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from types import TracebackType
from typing import Any

from pisky.bytes import Bytes
from pisky.tree.named.named_node import NamedNode
from pisky.tree.node import NodeConfig, RustNode


class NamedNodeConfig:
    """Attaches a name to any node config for display and error reporting.

    NamedNodeConfig is transparent - it delegates everything to its child.
    The name is only used when building a named tree for display.

    Example:
        config = AutoSamplingConfig([
            NamedNodeConfig("English", WeightedNodeConfig(
                RecordReaderConfig("en.disky"), 2.0
            )),
            NamedNodeConfig("German", WeightedNodeConfig(
                RecordReaderConfig("de.disky"), 1.0
            )),
        ])

        print(named_tree(config))
        # Root (3.0)
        # ├── English (2.0)
        # └── German (1.0)
    """

    def __init__(self, name: str, child: NodeConfig) -> None:
        if not name:
            raise ValueError("name cannot be empty")
        self._name = name
        self._child = child

    @property
    def name(self) -> str:
        """The name attached to this node."""
        return self._name

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
    def weight(self) -> float | None:
        """Delegate to child."""
        return self._child.weight

    def named_children(self) -> Sequence[NamedNode]:
        """Return this node as a NamedNode with children from subtree."""
        return [
            NamedNode(
                name=self._name,
                weight=self._child.weight,
                children=self._child.named_children(),
                terse=True,
            )
        ]

    def metadata(self) -> Sequence[Mapping[str, Any]]:
        """Pass through child's metadata."""
        return self._child.metadata()
