"""Tests for single-file RecordReaderConfig."""

import os
import tempfile
import pytest

from pisky import RecordWriterConfig, RecordReaderConfig, Zstd


class TestRecordReaderConfig:
    """Tests for RecordReaderConfig context manager API."""

    def test_read_basic(self):
        """Test basic read with context manager and iteration."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            temp_path = temp.name

        try:
            # Write test data
            with RecordWriterConfig(temp_path) as writer:
                writer.write(b"first")
                writer.write(b"second")
                writer.write(b"third")

            # Read with iteration
            records = []
            with RecordReaderConfig(temp_path) as reader:
                for record in reader:
                    records.append(bytes(record))

            assert records == [b"first", b"second", b"third"]
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_read_method(self):
        """Test explicit read() method."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            temp_path = temp.name

        try:
            with RecordWriterConfig(temp_path) as writer:
                writer.write(b"one")
                writer.write(b"two")

            with RecordReaderConfig(temp_path) as reader:
                record1 = reader.read()
                record2 = reader.read()
                record3 = reader.read()

            assert bytes(record1) == b"one"
            assert bytes(record2) == b"two"
            assert record3 is None  # EOF
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_read_empty_file(self):
        """Test reading an empty file."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            temp_path = temp.name

        try:
            with RecordWriterConfig(temp_path) as writer:
                pass  # Empty file

            records = []
            with RecordReaderConfig(temp_path) as reader:
                for record in reader:
                    records.append(bytes(record))

            assert records == []
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_read_compressed(self):
        """Test reading zstd compressed file."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            temp_path = temp.name

        try:
            with RecordWriterConfig(temp_path, compression=Zstd(3)) as writer:
                writer.write(b"compressed one")
                writer.write(b"compressed two")

            records = []
            with RecordReaderConfig(temp_path) as reader:
                for record in reader:
                    records.append(bytes(record))

            assert records == [b"compressed one", b"compressed two"]
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_read_many_records(self):
        """Test reading many records."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            temp_path = temp.name

        try:
            expected = [f"record_{i}".encode() for i in range(1000)]

            with RecordWriterConfig(temp_path) as writer:
                for record in expected:
                    writer.write(record)

            records = []
            with RecordReaderConfig(temp_path) as reader:
                for record in reader:
                    records.append(bytes(record))

            assert records == expected
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_read_binary_data(self):
        """Test reading binary data."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            temp_path = temp.name

        try:
            binary_data = bytes(range(256))

            with RecordWriterConfig(temp_path) as writer:
                writer.write(binary_data)

            records = []
            with RecordReaderConfig(temp_path) as reader:
                for record in reader:
                    records.append(bytes(record))

            assert records == [binary_data]
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_file_not_found(self):
        """Test error on non-existent file."""
        with pytest.raises(Exception) as exc_info:
            with RecordReaderConfig("/nonexistent/path/file.disky") as reader:
                pass

        assert "No such file" in str(exc_info.value) or "nonexistent" in str(exc_info.value)

if __name__ == "__main__":
    pytest.main(["-v", __file__])
