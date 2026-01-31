"""Protocols for pisky types."""

from typing import Protocol


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
