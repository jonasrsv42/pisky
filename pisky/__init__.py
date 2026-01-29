"""
Pisky: Python bindings for the Disky high-performance record format.

This module provides Python bindings for the Disky library,
which implements the Riegeli record format in Rust.
"""

import importlib.metadata

# New config-based API
from pisky.single import (
    RecordReader,
    RecordReaderConfig,
    RecordWriter,
    RecordWriterConfig,
    Zstd,
    Uncompressed,
)
from pisky.bytes import Bytes

# Import CorruptionStrategy, set_log_level, and PathType from common module
from .common import CorruptionStrategy, set_log_level, PathType

# Import MultiThreadedReader and MultiThreadedWriter from multi_threaded module
from .multi_threaded import MultiThreadedReader, MultiThreadedWriter

# Import Globable and expand_dirs from expand module
from .expand import Globable, expand_dirs

try:
    __version__ = importlib.metadata.version("pisky")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"

__all__ = [
    # New config-based API
    "RecordReaderConfig",
    "RecordWriterConfig",
    "RecordReader",
    "RecordWriter",
    "Zstd",
    "Uncompressed",
    "Bytes",
    # Legacy classes
    "MultiThreadedWriter",
    "MultiThreadedReader",
    "CorruptionStrategy",
    "Globable",
    # Functions
    "set_log_level",
    "expand_dirs",
    # Variables
    "__version__",
]
