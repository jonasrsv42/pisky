"""Corruption handling strategies for the Pisky library."""

from typing import Any
from pathlib import Path
from os import PathLike

from ._pisky import PyCorruptionStrategy

# Define a type for path-like objects
PathType = str | Path | PathLike[Any]


class CorruptionStrategy:
    """
    Enum for corruption handling strategies in Disky.

    Controls how the reader should handle corrupted data:
    - ERROR: Return an error when corruption is encountered (default)
    - RECOVER: Skip corrupted chunks and continue reading

    Example:
        from pisky import RecordReaderConfig, CorruptionStrategy

        with RecordReaderConfig("data.disky", CorruptionStrategy.RECOVER) as reader:
            for record in reader:
                process(record)
    """

    ERROR: "CorruptionStrategy"
    RECOVER: "CorruptionStrategy"

    def __init__(self, py_strategy: PyCorruptionStrategy) -> None:
        self._py_strategy = py_strategy

    def _to_py(self) -> PyCorruptionStrategy:
        """Get the underlying PyCorruptionStrategy for passing to Rust."""
        return self._py_strategy


# Initialize the class-level enum values
CorruptionStrategy.ERROR = CorruptionStrategy(PyCorruptionStrategy.Error)
CorruptionStrategy.RECOVER = CorruptionStrategy(PyCorruptionStrategy.Recover)