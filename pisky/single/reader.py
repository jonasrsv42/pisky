"""Single-file record reader with config-based API."""

from typing import Any
from pathlib import Path
from os import PathLike

from .._pisky import (
    RecordReaderConfig as _RecordReaderConfig,
    RecordReader as _RecordReader,
)
from ..bytes import Bytes

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


class RecordReaderConfig:
    """
    Configuration for a single-file record reader.

    This is a config object - it stores parameters but doesn't open any files
    until you enter the context manager.

    Example:
        from pisky import RecordReaderConfig

        with RecordReaderConfig("data.disky") as reader:
            for record in reader:
                process(bytes(record))

    Args:
        path: Path to the disky file.
        corruption_strategy: How to handle corrupted data.
            - None or "error": Raise an error on corruption (default)
            - "recover": Skip corrupted chunks and continue
    """

    def __init__(
        self,
        path: PathType,
        corruption_strategy: str | None = None,
    ) -> None:
        self._path = str(path)
        self._corruption_strategy = corruption_strategy
        self._config = _RecordReaderConfig(self._path, corruption_strategy)

    def __enter__(self) -> RecordReader:
        inner = self._config.__enter__()
        return RecordReader(inner)

    def __exit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        return False
