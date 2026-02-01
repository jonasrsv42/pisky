"""Protocols for pisky types."""

from typing import Protocol


class StrPath(Protocol):
    """Protocol for path-like objects that can be converted to string.

    This is more permissive than os.PathLike, accepting any object with __str__
    that returns a valid path string. This supports path protocols like
    datrasil.path.Path that don't implement __fspath__.

    Example:
        from pisky.protocol import StrPath

        def process_path(path: StrPath) -> None:
            path_str = str(path)
            ...
    """

    def __str__(self) -> str:
        """Return the string representation of the path."""
        ...


class Writer(Protocol):
    """Protocol for record writers.

    Both RecordWriter (single-file) and SequentialWriter (sharded) implement this.

    Example:
        from pisky.protocol import Writer

        def write_records(writer: Writer, records: list[bytes]) -> None:
            for record in records:
                writer.write(record)
    """

    def write(self, data: bytes) -> None:
        """Write a record."""
        ...
