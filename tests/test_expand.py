"""
Tests for the expand module.

This module tests the functionality of the Globable protocol and
expand_dirs function, which are used to work with sharded files
from MultiThreadedWriter.
"""

import os
import tempfile
import fnmatch
from pathlib import Path
import unittest
from typing import Iterator, List

from pisky import MultiThreadedWriter, expand_dirs, Globable


class CustomPath:
    """
    A custom path implementation that supports the Globable protocol.
    
    This class demonstrates how the Globable protocol can be used with
    custom path-like objects that aren't pathlib.Path.
    """
    
    def __init__(self, path: str):
        """Initialize with a string path."""
        self.path = path
    
    def __str__(self) -> str:
        """Return the string representation of the path."""
        return self.path
    
    def glob(self, pattern: str) -> Iterator['CustomPath']:
        """
        Find all paths matching the pattern.
        
        Returns custom path objects instead of pathlib.Path objects.
        """
        # Get all files in the directory
        try:
            files = os.listdir(self.path)
        except OSError:
            # If directory doesn't exist or isn't readable, return empty iterator
            return iter([])
        
        # Filter files based on the pattern
        for file in files:
            if fnmatch.fnmatch(file, pattern):
                yield CustomPath(os.path.join(self.path, file))


class TestExpand(unittest.TestCase):
    """Test case for the expand module."""

    def test_expand_dirs_with_pathlib(self):
        """Test expand_dirs with pathlib.Path directories."""
        # Create two temporary directories
        with tempfile.TemporaryDirectory() as temp_dir1, tempfile.TemporaryDirectory() as temp_dir2:
            dir1 = Path(temp_dir1)
            dir2 = Path(temp_dir2)
            
            # Write shards to the first directory
            with MultiThreadedWriter.new_with_shards(
                dir_path=dir1,
                prefix="shard",
                num_shards=2
            ) as writer:
                for i in range(10):
                    writer.write_record(f"Record1 #{i}".encode('utf-8'))
            
            # Write shards to the second directory with a different prefix
            with MultiThreadedWriter.new_with_shards(
                dir_path=dir2,
                prefix="custom",
                num_shards=3
            ) as writer:
                for i in range(10):
                    writer.write_record(f"Record2 #{i}".encode('utf-8'))
            
            # Verify shards were created
            shard_files1 = list(dir1.glob("shard_*"))
            self.assertEqual(len(shard_files1), 2)
            
            shard_files2 = list(dir2.glob("custom_*"))
            self.assertEqual(len(shard_files2), 3)
            
            # Test expand_dirs with default prefix
            all_shards = expand_dirs([dir1, dir2])
            self.assertEqual(len(all_shards), 2)  # Only finds "shard_*" prefix
            
            # Test expand_dirs with custom prefix
            custom_shards = expand_dirs([dir1, dir2], prefix="custom")
            self.assertEqual(len(custom_shards), 3)  # Only finds "custom_*" prefix
            
            # Ensure returned paths are Path objects
            for path in all_shards:
                self.assertIsInstance(path, Path)
    
    def test_expand_dirs_with_custom_path(self):
        """Test expand_dirs with custom path-like objects implementing Globable."""
        # Create two temporary directories
        with tempfile.TemporaryDirectory() as temp_dir1, tempfile.TemporaryDirectory() as temp_dir2:
            # Create custom path objects
            dir1 = CustomPath(temp_dir1)
            dir2 = CustomPath(temp_dir2)
            
            # Write shards to the first directory
            with MultiThreadedWriter.new_with_shards(
                dir_path=dir1.path,  # Use string path for MultiThreadedWriter
                prefix="shard",
                num_shards=2
            ) as writer:
                for i in range(10):
                    writer.write_record(f"Record1 #{i}".encode('utf-8'))
            
            # Write shards to the second directory with a different prefix
            with MultiThreadedWriter.new_with_shards(
                dir_path=dir2.path,  # Use string path for MultiThreadedWriter
                prefix="custom",
                num_shards=3
            ) as writer:
                for i in range(10):
                    writer.write_record(f"Record2 #{i}".encode('utf-8'))
            
            # Test expand_dirs with default prefix using custom path objects
            all_shards = expand_dirs([dir1, dir2])
            self.assertEqual(len(all_shards), 2)  # Only finds "shard_*" prefix
            
            # Test expand_dirs with custom prefix
            custom_shards = expand_dirs([dir1, dir2], prefix="custom")
            self.assertEqual(len(custom_shards), 3)  # Only finds "custom_*" prefix
            
            # Ensure returned paths are CustomPath objects
            for path in all_shards:
                self.assertIsInstance(path, CustomPath)
            
            # Verify the paths are correct
            shard_paths = [str(path) for path in all_shards]
            for path in shard_paths:
                self.assertTrue(os.path.exists(path))
                self.assertTrue("shard_" in path)


if __name__ == "__main__":
    unittest.main()
