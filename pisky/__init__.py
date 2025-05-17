"""
Pisky: Python bindings for the Disky high-performance record format.

This module provides Python bindings for the Disky library,
which implements the Riegeli record format in Rust.
"""

import importlib.metadata

# Import CorruptionStrategy, set_log_level, and PathType from common module
from .common import CorruptionStrategy, set_log_level, PathType

# Import Bytes, RecordReader, and RecordWriter from single_threaded module
from .single_threaded import Bytes, RecordReader, RecordWriter

# Import MultiThreadedReader and MultiThreadedWriter from multi_threaded module
from .multi_threaded import MultiThreadedReader, MultiThreadedWriter

# Import Globable and expand_dirs from expand module
from .expand import Globable, expand_dirs

try:
    __version__ = importlib.metadata.version("pisky")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"

__all__ = [
    # Classes
    "RecordWriter", 
    "RecordReader", 
    "MultiThreadedWriter", 
    "MultiThreadedReader",
    "Bytes", 
    "CorruptionStrategy",
    "Globable",
    # Functions
    "set_log_level",
    "expand_dirs",
    # Variables
    "__version__",
]