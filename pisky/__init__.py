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
)
from pisky.bytes import Bytes

# Import compression types
from .compression import Zstd, Uncompressed

# Import CorruptionStrategy and PathType from corruption module
from .corruption import CorruptionStrategy, PathType

# Import logging utilities
from .logging import set_log_level

# Import Globable and expand_dirs from expand module
from .expand import Globable, expand_dirs

# Import Writer protocol
from .protocol import Writer

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
    # Common
    "CorruptionStrategy",
    "Globable",
    "Writer",
    # Functions
    "set_log_level",
    "expand_dirs",
    # Variables
    "__version__",
]
