"""
Basic pytest tests for pisky's record readers and writers.

These tests verify that:
1. The single-threaded RecordWriter and RecordReader work correctly
2. The multi-threaded MultiThreadedWriter and MultiThreadedReader work correctly
"""

import os
import tempfile
import pytest
from pisky import RecordWriter, RecordReader, MultiThreadedWriter, MultiThreadedReader


class TestSingleThreaded:
    """Tests for single-threaded reader and writer."""

    def test_write_read_records(self):
        """Test basic writing and reading of records."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            temp_path = temp.name

        try:
            # Write some records
            with RecordWriter(temp_path) as writer:
                writer.write_record(b"Record 1")
                writer.write_record(b"Record 2")
                writer.write_record(b"Record 3")

            # Read the records back
            records = []
            with RecordReader(temp_path) as reader:
                for record in reader:
                    records.append(record.to_bytes())

            # Verify the records match
            assert len(records) == 3
            assert records[0] == b"Record 1"
            assert records[1] == b"Record 2"
            assert records[2] == b"Record 3"

        finally:
            # Clean up
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_manual_api(self):
        """Test using the manual API (without context managers)."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            temp_path = temp.name

        try:
            # Write some records manually
            writer = RecordWriter(temp_path)
            writer.write_record(b"Manual 1")
            writer.write_record(b"Manual 2")
            writer.flush()
            writer.close()

            # Read records manually
            reader = RecordReader(temp_path)
            records = []
            while True:
                record = reader.next_record()
                if record is None:
                    break
                records.append(record.to_bytes())

            # Verify the records match
            assert len(records) == 2
            assert records[0] == b"Manual 1"
            assert records[1] == b"Manual 2"

        finally:
            # Clean up
            if os.path.exists(temp_path):
                os.unlink(temp_path)


class TestMultiThreaded:
    """Tests for multi-threaded reader and writer."""

    def test_mt_write_read_records(self):
        """Test basic writing and reading with multi-threaded API."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Write records with multi-threaded writer
            with MultiThreadedWriter.new_with_shards(
                dir_path=temp_dir,
                # Using all default settings
            ) as writer:
                for i in range(100):
                    writer.write_record(f"MT Record {i}".encode())

            # Verify shard files were created
            shard_files = [f for f in os.listdir(temp_dir) if f.startswith("shard_")]
            assert len(shard_files) == 2  # Default is 2 shards

            # Read records with multi-threaded reader
            records = []
            with MultiThreadedReader.new_with_shards(
                dir_path=temp_dir,
                # Using all default settings
            ) as reader:
                for record in reader:
                    records.append(record.to_bytes())

            # Verify the records (count and content, but not order)
            assert len(records) == 100
            
            # Check that all records start with the expected prefix
            for record in records:
                assert record.startswith(b"MT Record")

    def test_mt_custom_settings(self):
        """Test multi-threaded API with custom settings."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Write records with custom settings
            with MultiThreadedWriter.new_with_shards(
                dir_path=temp_dir,
                prefix="custom",
                num_shards=3,
                worker_threads=2,
                task_queue_capacity=500,
                enable_auto_sharding=True,
            ) as writer:
                for i in range(200):
                    writer.write_record(f"Custom Record {i}".encode())

            # Verify shard files were created
            shard_files = [f for f in os.listdir(temp_dir) if f.startswith("custom_")]
            assert len(shard_files) == 3

            # Read records with custom settings
            records = []
            with MultiThreadedReader.new_with_shards(
                dir_path=temp_dir,
                prefix="custom",
                num_shards=3,
                worker_threads=2,
            ) as reader:
                for record in reader:
                    records.append(record.to_bytes())

            # Verify the records
            assert len(records) == 200
            for i, record in enumerate(records):
                assert record.startswith(b"Custom Record")
                
    def test_mt_reading_writing(self):
        """Test multi-threaded API with read and write operations."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Write in one process
            with MultiThreadedWriter.new_with_shards(
                dir_path=temp_dir,
                prefix="readwrite",
                num_shards=2,
            ) as writer:
                for i in range(100):
                    writer.write_record(f"ReadWrite Record {i}".encode())
            
            # Read in another process
            records = []
            with MultiThreadedReader.new_with_shards(
                dir_path=temp_dir,
                prefix="readwrite",
                num_shards=2,
            ) as reader:
                for record in reader:
                    records.append(record.to_bytes())
            
            # Verify record count
            assert len(records) == 100
            
            # Verify content of records without assuming specific order
            expected_records = set([f"ReadWrite Record {i}".encode() for i in range(100)])
            assert set(records) == expected_records


if __name__ == "__main__":
    pytest.main(["-v", __file__])
