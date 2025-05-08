"""
Common types and utilities for the Pisky library.

This module contains shared functionality and type definitions used
across the Pisky library.
"""

from typing import Union, Protocol, Any
from pathlib import Path
from os import PathLike

from ._pisky import PyCorruptionStrategy, set_log_level as _set_log_level

# Define a type for path-like objects
PathType = Union[str, Path, PathLike, Any]

# Define a Python enum for CorruptionStrategy
class CorruptionStrategy:
    """
    Enum for corruption handling strategies in Disky.
    
    Controls how the reader should handle corrupted data:
    - ERROR: Return an error when corruption is encountered (default)
    - RECOVER: Skip corrupted chunks and continue reading
    """
    ERROR = PyCorruptionStrategy.Error
    RECOVER = PyCorruptionStrategy.Recover


def set_log_level(level_str: str) -> None:
    """
    Set the logging level for the Disky library.
    
    Args:
        level_str: One of "trace", "debug", "info", "warn", "error", or "off"
        
    Raises:
        IOError: If an invalid log level is provided
    """
    _set_log_level(level_str)