"""Node protocol for tree composition."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

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
    - _to_rust_node(): Build Rust tree for execution
    - weight: Subtree weight (None if unweighted path)

    Weight rules:
    - WeightedNode wrappers are additive (stacked weights sum)
    - Pass-through nodes (Shuffle, Threaded) return child's weight
    - Multi-child nodes (RoundRobin) sum children weights
    - SamplingConfig is a weighted sum: sum(explicit_w * subtree_weight)
    - Leaf nodes without weight return None
    """

    def _to_rust_node(self) -> RustNode:
        """Convert this config to its Rust equivalent for tree composition."""
        ...

    @property
    def weight(self) -> float | None:
        """Return subtree weight, or None if unweighted.

        Returns:
            float - Computed subtree weight
            None - No weight in this subtree (unweighted path)
        """
        ...
