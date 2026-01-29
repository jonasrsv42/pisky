"""Tests for single-file RecordWriterConfig."""

import os
import tempfile
import pytest

from pisky import RecordWriterConfig, RecordReaderConfig, Zstd, Uncompressed


class TestRecordWriterConfig:
    """Tests for RecordWriterConfig context manager API."""

    def test_write_basic(self):
        """Test basic write with context manager."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            temp_path = temp.name

        try:
            with RecordWriterConfig(temp_path) as writer:
                writer.write(b"hello")
                writer.write(b"world")

            # Verify by reading back
            records = []
            with RecordReaderConfig(temp_path) as reader:
                for record in reader:
                    records.append(bytes(record))

            assert records == [b"hello", b"world"]
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_write_with_zstd(self):
        """Test write with Zstd compression."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            temp_path = temp.name

        try:
            with RecordWriterConfig(temp_path, compression=Zstd(3)) as writer:
                writer.write(b"compressed data")
                writer.write(b"more compressed data")

            # Verify by reading back
            records = []
            with RecordReaderConfig(temp_path) as reader:
                for record in reader:
                    records.append(bytes(record))

            assert records == [b"compressed data", b"more compressed data"]
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_write_with_zstd_default_level(self):
        """Test Zstd with default compression level."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            temp_path = temp.name

        try:
            with RecordWriterConfig(temp_path, compression=Zstd()) as writer:
                writer.write(b"default level")

            records = []
            with RecordReaderConfig(temp_path) as reader:
                for record in reader:
                    records.append(bytes(record))

            assert records == [b"default level"]
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_write_with_uncompressed(self):
        """Test write with explicit Uncompressed."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            temp_path = temp.name

        try:
            with RecordWriterConfig(temp_path, compression=Uncompressed()) as writer:
                writer.write(b"uncompressed")

            records = []
            with RecordReaderConfig(temp_path) as reader:
                for record in reader:
                    records.append(bytes(record))

            assert records == [b"uncompressed"]
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_compression_reduces_size(self):
        """Test that compression actually reduces file size for repetitive data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repetitive_data = b"COMPRESS_ME_" * 1000

            # Write uncompressed
            uncompressed_path = os.path.join(tmpdir, "uncompressed.disky")
            with RecordWriterConfig(uncompressed_path, compression=Uncompressed()) as writer:
                writer.write(repetitive_data)

            # Write compressed
            compressed_path = os.path.join(tmpdir, "compressed.disky")
            with RecordWriterConfig(compressed_path, compression=Zstd(3)) as writer:
                writer.write(repetitive_data)

            uncompressed_size = os.path.getsize(uncompressed_path)
            compressed_size = os.path.getsize(compressed_path)

            assert compressed_size < uncompressed_size * 0.5

    def test_write_empty_file(self):
        """Test creating an empty file."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            temp_path = temp.name

        try:
            with RecordWriterConfig(temp_path) as writer:
                pass  # Don't write anything

            records = []
            with RecordReaderConfig(temp_path) as reader:
                for record in reader:
                    records.append(bytes(record))

            assert records == []
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_write_many_records(self):
        """Test writing many records."""
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

    def test_write_binary_data(self):
        """Test writing binary data with all byte values."""
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

    def test_flush(self):
        """Test explicit flush."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            temp_path = temp.name

        try:
            with RecordWriterConfig(temp_path) as writer:
                writer.write(b"before flush")
                writer.flush()
                writer.write(b"after flush")

            records = []
            with RecordReaderConfig(temp_path) as reader:
                for record in reader:
                    records.append(bytes(record))

            assert records == [b"before flush", b"after flush"]
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)


class TestZstdRepr:
    """Test compression type representations."""

    def test_zstd_repr(self):
        """Test Zstd repr."""
        assert repr(Zstd(3)) == "Zstd(3)"
        assert repr(Zstd(9)) == "Zstd(9)"

    def test_uncompressed_repr(self):
        """Test Uncompressed repr."""
        assert repr(Uncompressed()) == "Uncompressed()"


if __name__ == "__main__":
    pytest.main(["-v", __file__])
