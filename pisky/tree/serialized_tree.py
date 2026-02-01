"""Serialized tree reader for pisky.

This module provides the tree_from_bytes function for deserializing
tree configs from bytes.
"""

from types import TracebackType

from pisky._pisky import tree_from_bytes as _tree_from_bytes
from pisky.tree.round_robin import TreeReader


class SerializedTreeReader:
    """Context manager for reading from a serialized tree config.

    Use tree_from_bytes() to create this from serialized config bytes.

    Example:
        serialized = config.serialize()
        with tree_from_bytes(serialized) as reader:
            for record in reader:
                process(bytes(record))
    """

    def __init__(self, inner: object) -> None:
        # inner is the Rust TreeReader from _tree_from_bytes
        self._inner = inner
        self._reader: TreeReader | None = None

    def __enter__(self) -> TreeReader:
        self._reader = TreeReader(self._inner)
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


def tree_from_bytes(serialized_config: bytes) -> SerializedTreeReader:
    """Deserialize a tree config and return a context manager for reading.

    Args:
        serialized_config: Bytes from a config's serialize() method.

    Returns:
        A context manager that yields a TreeReader.

    Raises:
        ValueError: If the serialized config is invalid.

    Example:
        serialized = config.serialize()
        with tree_from_bytes(serialized) as reader:
            for record in reader:
                process(bytes(record))
    """
    # Validate and build the reader eagerly so errors are raised here
    inner = _tree_from_bytes(serialized_config)
    return SerializedTreeReader(inner)
