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
from pathlib import Path

import pytest

from pisky import RecordReaderConfig, RecordWriterConfig


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
        """Test RecordWriterConfig and RecordReaderConfig with string paths."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            temp_path = temp.name

        try:
            # Use the string path
            with RecordWriterConfig(temp_path) as writer:
                writer.write(b"String path test")

            with RecordReaderConfig(temp_path) as reader:
                record = reader.read()
                assert bytes(record) == b"String path test"

        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)


class TestPathlibPaths:
    """Tests for using pathlib.Path objects with pisky."""

    def test_record_writer_reader_pathlib_path(self):
        """Test RecordWriterConfig and RecordReaderConfig with pathlib.Path objects."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            temp_path = Path(temp.name)

        try:
            # Use pathlib.Path object
            with RecordWriterConfig(temp_path) as writer:
                writer.write(b"Pathlib path test")

            with RecordReaderConfig(temp_path) as reader:
                record = reader.read()
                assert bytes(record) == b"Pathlib path test"

        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)


class TestCustomPathObjects:
    """Tests for using custom objects with __str__ method as paths."""

    def test_record_writer_reader_custom_path(self):
        """Test RecordWriterConfig and RecordReaderConfig with custom path-like objects."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            # Create custom path object
            custom_path = CustomPathLike(temp.name)

        try:
            # Use custom path object
            with RecordWriterConfig(custom_path) as writer:
                writer.write(b"Custom path test")

            with RecordReaderConfig(custom_path) as reader:
                record = reader.read()
                assert bytes(record) == b"Custom path test"

        finally:
            if os.path.exists(str(custom_path)):
                os.unlink(str(custom_path))


class TestInvalidPaths:
    """Tests for handling invalid path objects."""

    def test_invalid_object_record_writer(self):
        """Test that RecordWriterConfig properly rejects invalid path objects."""
        # Create an invalid path object
        invalid_path = InvalidPathObject()

        # Attempt to use with RecordWriterConfig
        with pytest.raises(TypeError):
            RecordWriterConfig(invalid_path)


if __name__ == "__main__":
    pytest.main(["-v", __file__])
