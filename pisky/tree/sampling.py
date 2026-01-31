"""Sampling reader for weighted random sampling from multiple sources."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from pisky._pisky import SamplingConfig as _SamplingConfig
from pisky.tree.node import NodeConfig, RustNode
from pisky.tree.round_robin import TreeReader


class SamplingConfig:
    """
    Sampling reader - samples from multiple sources based on weights.

    Takes a sequence of (node, weight) pairs and samples from them based on
    the weights. Higher weights mean the source is sampled more frequently.

    Example:
        from pisky.single import RecordReaderConfig
        from pisky.tree import SamplingConfig

        config = SamplingConfig(
            [
                (RecordReaderConfig("train.disky"), 2.0),
                (RecordReaderConfig("valid.disky"), 1.0),
            ],
            seed=42,
        )
        with config as reader:
            for record in reader:
                # Records sampled 2:1 from train vs valid
                process(bytes(record))

    Nested composition:
        config = SamplingConfig(
            [
                (ShuffleConfig(RecordReaderConfig("a.disky"), buffer_size=100), 3.0),
                (RecordReaderConfig("b.disky"), 1.0),
            ],
            seed=42,
        )
    """

    def __init__(
        self,
        sources: Sequence[tuple[NodeConfig, float]],
        seed: int | None = None,
    ) -> None:
        """
        Create a sampling config.

        Args:
            sources: List of (node, weight) tuples. Weights must be positive.
                Higher weights mean the source is sampled more frequently.
            seed: Optional random seed for deterministic sampling.

        Raises:
            ValueError: If sources is empty or any weight is non-positive.
        """
        if not sources:
            raise ValueError("sources cannot be empty")
        if any(weight <= 0 for _, weight in sources):
            raise ValueError("all weights must be positive")

        self._sources = sources
        self._seed = seed
        self._reader: TreeReader | None = None

    def __enter__(self) -> TreeReader:
        rust_sources = [
            (child._to_rust_node(), weight) for child, weight in self._sources
        ]
        rust_config = _SamplingConfig(rust_sources, self._seed)
        inner = rust_config.__enter__()
        self._reader = TreeReader(inner)
        return self._reader

    def __exit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        if self._reader is not None:
            self._reader.close()
            self._reader = None
        return False

    def _to_rust_node(self) -> RustNode:
        """Convert this config to its Rust equivalent for tree composition."""
        rust_sources = [
            (child._to_rust_node(), weight) for child, weight in self._sources
        ]
        return _SamplingConfig(rust_sources, self._seed)
