"""AutoSamplingConfig for automatic weight-based sampling."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import TracebackType
from typing import Any

from pisky._pisky import SamplingConfig as _SamplingConfig
from pisky.tree.named.named_node import NamedNode
from pisky.tree.named.tree import format_weight_error
from pisky.tree.node import NodeConfig, RustNode
from pisky.tree.round_robin import TreeReader


class AutoSamplingConfig:
    """Samples from children using weights computed from subtrees.

    Each child's subtree weight is used as its sampling weight.
    All children must have weights (via WeightedNodeConfig or LazyWeightedNodeConfig).

    Example:
        config = AutoSamplingConfig([
            WeightedNodeConfig(RecordReaderConfig("en.disky"), 2.0),
            WeightedNodeConfig(RecordReaderConfig("de.disky"), 1.0),
        ])
        # Samples 2:1 from en vs de

    Complex example with nested weights:
        config = AutoSamplingConfig([
            ThreadedConfig(WeightedNodeConfig(ShardReaderConfig("/data/en"), 2.0)),
            ThreadedConfig(RoundRobinConfig([
                WeightedNodeConfig(ShardReaderConfig("/data/de/news"), 3.0),
                WeightedNodeConfig(ShardReaderConfig("/data/de/books"), 1.0),
            ])),
        ])
        # en: weight=2.0, de: weight=3.0+1.0=4.0
        # Samples 2:4 (1:2) from en vs de
    """

    def __init__(
        self,
        children: Sequence[NodeConfig],
        seed: int | None = None,
    ) -> None:
        if not children:
            raise ValueError("children cannot be empty")
        self._children = list(children)
        self._seed = seed
        self._sampling: _SamplingConfig | None = None
        self._reader: TreeReader | None = None

    def _validate_children(self) -> list[tuple[NodeConfig, float]]:
        """Validate all children have weights and return (child, weight) pairs."""
        result: list[tuple[NodeConfig, float]] = []
        missing: list[int] = []

        for i, child in enumerate(self._children):
            w = child.weight
            if w is None:
                missing.append(i)
            else:
                result.append((child, w))

        if missing:
            raise ValueError(
                f"AutoSamplingConfig requires all children to have weights. "
                f"Children at indices {missing} are missing weights. "
                f"Wrap them with WeightedNodeConfig."
            )

        return result

    def __enter__(self) -> TreeReader:
        # Validate and get (child, weight) pairs (lazy nodes resolve on weight access)
        validated = self._validate_children()

        # Build SamplingConfig with computed weights
        rust_sources = [
            (child._to_rust_node(), w)
            for child, w in validated
        ]
        self._sampling = _SamplingConfig(rust_sources, self._seed)
        inner = self._sampling.__enter__()
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
        self._sampling = None
        return False

    def _to_rust_node(self) -> RustNode:
        """Build Rust sampling node with computed weights."""
        validated = self._validate_children()
        rust_sources = [
            (child._to_rust_node(), w)
            for child, w in validated
        ]
        return _SamplingConfig(rust_sources, self._seed)

    def serialize(self) -> bytes:
        """Serialize this config to bytes for cross-library transfer."""
        return self._to_rust_node().serialize_as_bytes()

    @property
    def weight(self) -> float | None:
        """Subtree weight (sum of children weights)."""
        child_weights = [c.weight for c in self._children]

        if all(w is None for w in child_weights):
            return None

        if any(w is None for w in child_weights):
            raise ValueError(
                format_weight_error("AutoSamplingConfig", self._children)
            )

        return sum(child_weights)  # type: ignore

    def named_children(self) -> Sequence[NamedNode]:
        """Return this node with children from all child nodes."""
        all_children: list[NamedNode] = []
        for child in self._children:
            all_children.extend(child.named_children())
        metadata = {}
        if self._seed is not None:
            metadata["seed"] = self._seed
        return [
            NamedNode(
                name=self.__class__.__name__,
                weight=self.weight,
                children=all_children,
                metadata=metadata,
            )
        ]

    def metadata(self) -> Sequence[Mapping[str, Any]]:
        """Collect metadata from all children."""
        result: list[Mapping[str, Any]] = []
        for child in self._children:
            result.extend(child.metadata())
        return result
