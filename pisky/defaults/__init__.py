"""
Sane defaults for common pisky usage patterns.

This module provides convenience functions for the most common use cases,
with sensible defaults that work well for most scenarios.
"""

from pisky.defaults.single_threaded import read_shards, write_shards, count_shards
from pisky.defaults.multi_threaded import (
    count_shards_parallel,
    read_shards_parallel,
    write_shards_parallel,
)

__all__ = [
    "read_shards",
    "write_shards",
    "count_shards",
    "read_shards_parallel",
    "write_shards_parallel",
    "count_shards_parallel",
]
