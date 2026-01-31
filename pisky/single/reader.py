"""Single-file record reader with config-based API."""

from os import PathLike
from pathlib import Path
from types import TracebackType
from typing import Any

from collections.abc import Sequence

from .._pisky import RecordReader as _RecordReader
from .._pisky import RecordReaderConfig as _RecordReaderConfig
from ..bytes import Bytes
from ..corruption import CorruptionStrategy
from ..tree.named.named_node import NamedNode
from ..tree.node import RustNode

PathType = str | Path | PathLike[Any]


class RecordReader:
    """
    Active record reader for iterating over records.

    Created by entering a RecordReaderConfig context manager.
    Supports iteration protocol.

    Example:
        with RecordReaderConfig("data.disky") as reader:
            for record in reader:
                print(bytes(record))
    """

    def __init__(self, inner: _RecordReader) -> None:
        self._inner = inner

    def read(self) -> Bytes | None:
        """
        Read the next record.

        Returns:
            Record bytes, or None if EOF reached.
        """
        return self._inner.read()

    def __iter__(self) -> "RecordReader":
        return self

    def __next__(self) -> Bytes:
        record = self._inner.read()
        if record is None:
            raise StopIteration
        return record

    def _close(self) -> None:
        """Close the reader. Called automatically on context exit."""
        self._inner.close()


class RecordReaderConfig:
    """
    Configuration for a single-file record reader.

    This is a config object - it stores parameters but doesn't open any files
    until you enter the context manager.

    Example:
        from pisky import RecordReaderConfig, CorruptionStrategy

        with RecordReaderConfig("data.disky") as reader:
            for record in reader:
                process(bytes(record))

        # With corruption recovery
        with RecordReaderConfig("data.disky", CorruptionStrategy.RECOVER) as reader:
            for record in reader:
                process(bytes(record))

    Args:
        path: Path to the disky file.
        corruption_strategy: How to handle corrupted data.
            - CorruptionStrategy.ERROR: Raise an error on corruption (default)
            - CorruptionStrategy.RECOVER: Skip corrupted chunks and continue
    """

    def __init__(
        self,
        path: PathType,
        corruption_strategy: CorruptionStrategy = CorruptionStrategy.ERROR,
    ) -> None:
        self._path = str(path)
        self._corruption_strategy = corruption_strategy
        self._config = _RecordReaderConfig(self._path, corruption_strategy._to_py())
        self._reader: RecordReader | None = None

    def __enter__(self) -> RecordReader:
        inner = self._config.__enter__()
        self._reader = RecordReader(inner)
        return self._reader

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        if self._reader is not None:
            self._reader._close()
            self._reader = None
        return False

    def _to_rust_node(self) -> RustNode:
        """Convert this config to its Rust equivalent for tree composition."""
        return self._config

    @property
    def weight(self) -> float | None:
        """Leaf node - no weight."""
        return None

    def named_children(self) -> Sequence[NamedNode]:
        """Return this node as a leaf."""
        return [
            NamedNode(
                name=self.__class__.__name__,
                weight=self.weight,
                children=[],
                metadata={"path": self._path},
            )
        ]

    @staticmethod
    def count_records(
        path: PathType,
        corruption_strategy: CorruptionStrategy = CorruptionStrategy.ERROR,
    ) -> int:
        """
        Count records in a file without fully loading them into memory.

        Args:
            path: Path to the disky file.
            corruption_strategy: How to handle corrupted data.

        Returns:
            Number of records in the file.
        """
        count = 0
        with RecordReaderConfig(path, corruption_strategy) as reader:
            for _ in reader:
                count += 1
        return count
