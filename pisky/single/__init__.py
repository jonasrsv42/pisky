"""Single-file reader and writer with config-based API."""

from .reader import RecordReader, RecordReaderConfig
from .writer import RecordWriter, RecordWriterConfig, Zstd, Uncompressed

__all__ = [
    "RecordReader",
    "RecordReaderConfig",
    "RecordWriter",
    "RecordWriterConfig",
    "Zstd",
    "Uncompressed",
]
