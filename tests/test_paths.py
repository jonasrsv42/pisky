"""
Tests for path handling in pisky's readers and writers.

These tests verify that:
1. String paths work correctly
2. pathlib.Path objects work correctly
3. Custom objects with __str__ methods work correctly
4. Invalid path objects are rejected properly
"""

import os
import tempfile
import pytest
from pathlib import Path
from pisky import RecordWriter, RecordReader, MultiThreadedWriter, MultiThreadedReader


class CustomPathLike:
    """A custom class that implements a path-like interface via __str__."""
    
    def __init__(self, path):
        self.path = path
        
    def __str__(self):
        return str(self.path)


class InvalidPathObject:
    """A class that raises an error when str() is called."""
    
    def __init__(self):
        self.value = 42
    
    def __str__(self):
        raise TypeError("Cannot convert this object to a path string")


class TestStringPaths:
    """Tests for using string paths with pisky."""
    
    def test_record_writer_reader_string_path(self):
        """Test RecordWriter and RecordReader with string paths."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            temp_path = temp.name
            
        try:
            # Use the string path
            with RecordWriter(temp_path) as writer:
                writer.write_record(b"String path test")
                
            with RecordReader(temp_path) as reader:
                record = reader.next_record()
                assert record.to_bytes() == b"String path test"
                
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_multithreaded_string_path(self):
        """Test MultiThreadedWriter and MultiThreadedReader with string paths."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Write records with string path
            with MultiThreadedWriter.new_with_shards(
                dir_path=temp_dir,
                prefix="str_path",
                num_shards=1
            ) as writer:
                writer.write_record(b"MT string path test")
                
            # Read records with string path
            with MultiThreadedReader.new_with_shards(
                dir_path=temp_dir,
                prefix="str_path",
                num_shards=1
            ) as reader:
                record = reader.next_record()
                assert record.to_bytes() == b"MT string path test"


class TestPathlibPaths:
    """Tests for using pathlib.Path objects with pisky."""
    
    def test_record_writer_reader_pathlib_path(self):
        """Test RecordWriter and RecordReader with pathlib.Path objects."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            temp_path = Path(temp.name)
            
        try:
            # Use pathlib.Path object
            with RecordWriter(temp_path) as writer:
                writer.write_record(b"Pathlib path test")
                
            with RecordReader(temp_path) as reader:
                record = reader.next_record()
                assert record.to_bytes() == b"Pathlib path test"
                
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_multithreaded_pathlib_path(self):
        """Test MultiThreadedWriter and MultiThreadedReader with pathlib.Path objects."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Convert to Path object
            path_obj = Path(temp_dir)
            
            # Write records with Path object
            with MultiThreadedWriter.new_with_shards(
                dir_path=path_obj,
                prefix="path_obj",
                num_shards=1
            ) as writer:
                writer.write_record(b"MT pathlib path test")
                
            # Read records with Path object
            with MultiThreadedReader.new_with_shards(
                dir_path=path_obj,
                prefix="path_obj",
                num_shards=1
            ) as reader:
                record = reader.next_record()
                assert record.to_bytes() == b"MT pathlib path test"


class TestCustomPathObjects:
    """Tests for using custom objects with __str__ method as paths."""
    
    def test_record_writer_reader_custom_path(self):
        """Test RecordWriter and RecordReader with custom path-like objects."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            # Create custom path object
            custom_path = CustomPathLike(temp.name)
            
        try:
            # Use custom path object
            with RecordWriter(custom_path) as writer:
                writer.write_record(b"Custom path test")
                
            with RecordReader(custom_path) as reader:
                record = reader.next_record()
                assert record.to_bytes() == b"Custom path test"
                
        finally:
            if os.path.exists(str(custom_path)):
                os.unlink(str(custom_path))
    
    def test_multithreaded_custom_path(self):
        """Test MultiThreadedWriter and MultiThreadedReader with custom path-like objects."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create custom path object
            custom_path = CustomPathLike(temp_dir)
            
            # Write records with custom path object
            with MultiThreadedWriter.new_with_shards(
                dir_path=custom_path,
                prefix="custom_path",
                num_shards=1
            ) as writer:
                writer.write_record(b"MT custom path test")
                
            # Read records with custom path object
            with MultiThreadedReader.new_with_shards(
                dir_path=custom_path,
                prefix="custom_path",
                num_shards=1
            ) as reader:
                record = reader.next_record()
                assert record.to_bytes() == b"MT custom path test"


class TestInvalidPaths:
    """Tests for handling invalid path objects."""
    
    def test_invalid_object_record_writer(self):
        """Test that RecordWriter properly rejects invalid path objects."""
        # Create an invalid path object
        invalid_path = InvalidPathObject()
        
        # Attempt to use with RecordWriter
        with pytest.raises(TypeError):
            writer = RecordWriter(invalid_path)
    
    def test_invalid_object_multithreaded_writer(self):
        """Test that MultiThreadedWriter properly rejects invalid path objects."""
        # Create an invalid path object
        invalid_path = InvalidPathObject()
        
        # Attempt to use with MultiThreadedWriter
        with pytest.raises(TypeError):
            writer = MultiThreadedWriter.new_with_shards(dir_path=invalid_path)


if __name__ == "__main__":
    pytest.main(["-v", __file__])