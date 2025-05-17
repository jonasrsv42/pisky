"""
Utility functions for working with Disky shards.

This module provides utilities for manipulating and working with
Disky shard files, particularly for expanding directories written
by MultiThreadedWriter into individual shard files.
"""

from typing import Sequence, List, Protocol, TypeVar, Iterator, Any


T = TypeVar('T')


class Globable(Protocol[T]):
    """
    Protocol defining objects that can be used with glob operations.
    
    This allows for flexible usage with various path-like objects such as
    pathlib.Path or any object that implements a glob method returning an
    iterator of path-like objects of the same type.
    """
    def glob(self, pattern: str) -> Iterator[T]:
        """
        Find all paths matching the pattern.
        
        Args:
            pattern: The pattern to match against
            
        Returns:
            An iterator yielding matching paths of the same type
        """
        ...


G = TypeVar('G', bound=Globable)


def expand_dirs(directories: Sequence[G], prefix: str = "shard") -> List[Any]:
    """
    Expand a sequence of directories to a flattened list of all shard files.
    
    This function is useful for working with directories created by 
    MultiThreadedWriter to get a list of all individual shard files.
    
    Args:
        directories: Sequence of directories to expand. Each directory
            must implement the Globable protocol (i.e., have a glob method).
        prefix: Prefix for shard file names (default: "shard")
            
    Returns:
        A flattened list of all shard files found in the given directories.
        The return type will match the return type of the glob method.
        
    Example:
        ```python
        from pisky import MultiThreadedWriter, expand_dirs
        from pathlib import Path
        
        # Write data to multiple directories
        dirs = [Path("/tmp/output1"), Path("/tmp/output2")]
        for dir_path in dirs:
            with MultiThreadedWriter.new_with_shards(
                dir_path=dir_path,
                prefix="shard",
                num_shards=3
            ) as writer:
                for i in range(100):
                    writer.write_record(f"Record for {dir_path} #{i}".encode('utf-8'))
        
        # Get a flattened list of all shard files
        all_shards = expand_dirs(dirs)
        print(f"Found {len(all_shards)} total shards")
        
        # Use with custom prefix
        custom_shards = expand_dirs(dirs, prefix="custom")
        ```
    """
    result = []
    
    for directory in directories:
        # Use the directory's glob method to find all matching files
        # The pattern matches files starting with prefix_ (e.g., "shard_*")
        matching_files = directory.glob(f"{prefix}_*")
        
        # Add all matches to the result list
        result.extend(matching_files)
    
    return result