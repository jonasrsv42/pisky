"""Node protocol for tree composition."""

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

    Any config that implements `_to_rust_node()` can be composed in a tree.
    """

    def _to_rust_node(self) -> RustNode:
        """Convert this config to its Rust equivalent for tree composition."""
        ...
