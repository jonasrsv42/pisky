"""Node protocol for tree composition."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol, runtime_checkable

from pisky.tree.named.named_node import NamedNode

from pisky._pisky import RecordReaderConfig as RustRecordReaderConfig
from pisky._pisky import RoundRobinConfig as RustRoundRobinConfig
from pisky._pisky import RoundRobinReaderRandomOrderConfig
from pisky._pisky import RoundRobinReaderSequentialOrderConfig
from pisky._pisky import SamplingConfig as RustSamplingConfig
from pisky._pisky import SequentialReaderRandomOrderConfig
from pisky._pisky import SequentialReaderSequentialOrderConfig
from pisky._pisky import ShuffleConfig as RustShuffleConfig
from pisky._pisky import ThreadedConfig as RustThreadedConfig

RustNode = (
    SequentialReaderSequentialOrderConfig
    | SequentialReaderRandomOrderConfig
    | RoundRobinReaderSequentialOrderConfig
    | RoundRobinReaderRandomOrderConfig
    | RustRoundRobinConfig
    | RustRecordReaderConfig
    | RustSamplingConfig
    | RustShuffleConfig
    | RustThreadedConfig
)


@runtime_checkable
class NodeConfig(Protocol):
    """Protocol for configs that can be used as tree nodes.

    All nodes implement:
    - _to_rust_node(): Build Rust node (internal)
    - serialize(): Serialize config to bytes for cross-library transfer
    - weight: Subtree weight (None if unweighted path)

    Weight rules:
    - WeightedNode wrappers are additive (stacked weights sum)
    - Pass-through nodes (Shuffle, Threaded) return child's weight
    - Multi-child nodes (RoundRobin) sum children weights
    - SamplingConfig is a weighted sum: sum(explicit_w * subtree_weight)
    - Leaf nodes without weight return None
    """

    def _to_rust_node(self) -> RustNode:
        """Convert this config to its Rust equivalent for tree composition.

        Internal method - use serialize() for cross-library transfer.
        """
        ...

    def serialize(self) -> bytes:
        """Serialize this config to bytes for cross-library transfer.

        Use tree_from_bytes() to deserialize and build a reader.
        """
        ...

    @property
    def weight(self) -> float | None:
        """Return subtree weight, or None if unweighted.

        Returns:
            float - Computed subtree weight
            None - No weight in this subtree (unweighted path)
        """
        ...

    def named_children(self) -> Sequence[NamedNode]:
        """Gather named nodes from this subtree.

        Returns:
            Sequence of NamedNode from NamedNodeConfig nodes in subtree.
            Empty sequence if no named nodes in subtree.
        """
        ...

    def metadata(self) -> Sequence[Mapping[str, Any]]:
        """Collect metadata from all leaves in this subtree.

        Returns:
            Sequence of metadata mappings, one per leaf node.
            Leaf nodes return [{}] or [{...}] with their metadata.
            Pass-through nodes (Shuffle, Threaded) return child's metadata.
            Multi-child nodes (Sampling, RoundRobin) extend from all children.

        Common metadata keys (by convention):
            - "provenance": Data source version/origin for consistency checking
            - "path": Path to data source
        """
        ...
