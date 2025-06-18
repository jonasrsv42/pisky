"""
Basic pytest tests for pisky's record readers and writers.

These tests verify that:
1. The single-threaded RecordWriter and RecordReader work correctly
2. The multi-threaded MultiThreadedWriter and MultiThreadedReader work correctly
"""

import os
import tempfile
import pytest
from pisky import RecordWriter, RecordReader, MultiThreadedWriter, MultiThreadedReader, CorruptionStrategy


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
                
    def test_count_records(self):
        """Test counting records using the static count_records method."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            temp_path = temp.name

        try:
            # Write a variable number of records
            num_records = 100
            with RecordWriter(temp_path) as writer:
                for i in range(num_records):
                    writer.write_record(f"Count Record {i}".encode())

            # Count records without loading them all into memory
            count = RecordReader.count_records(temp_path)
            assert count == num_records
            
            # Verify by reading and counting records manually
            records = []
            with RecordReader(temp_path) as reader:
                for record in reader:
                    records.append(record.to_bytes())
            
            assert len(records) == num_records
            assert count == len(records)

        finally:
            # Clean up
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_count_records_empty_file(self):
        """Test counting records on an empty file."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            temp_path = temp.name

        try:
            # Create an empty file with no records
            with RecordWriter(temp_path) as writer:
                pass  # Don't write any records
            
            # Count records
            count = RecordReader.count_records(temp_path)
            assert count == 0
            
            # Verify by reading
            records = []
            with RecordReader(temp_path) as reader:
                for record in reader:
                    records.append(record.to_bytes())
            
            assert len(records) == 0

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
                
            # Test counting records without loading them all into memory
            record_count = MultiThreadedReader.count_records_with_shards(
                dir_path=temp_dir,
                # Using all default settings
            )
            assert record_count == 100
            
            # Get the full shard paths
            full_shard_paths = [os.path.join(temp_dir, shard) for shard in shard_files]
            
            # Test counting records with specific shard paths
            record_count_paths = MultiThreadedReader.count_records_with_shard_paths(
                shard_paths=full_shard_paths,
                # Using all default settings
            )
            assert record_count_paths == 100
            
            # Test with corruption recovery strategy
            record_count_recover = MultiThreadedReader.count_records_with_shards(
                dir_path=temp_dir,
                corruption_strategy=CorruptionStrategy.RECOVER
            )
            assert record_count_recover == 100
            
            # Counts should be the same with or without recovery strategy for non-corrupted files
            assert record_count == record_count_recover

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


class TestCompression:
    """Tests for compression support."""

    def test_single_threaded_zstd_compression(self):
        """Test single-threaded writer and reader with zstd compression."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            temp_path = temp.name

        try:
            # Test records with varying compression characteristics
            test_records = [
                b"Short record",
                b"Repetitive " * 100,  # Should compress well
                bytes(range(256)),     # Binary data
                b"",                   # Empty record
                b"A" * 10000,         # Large repetitive record
            ]

            # Write with zstd compression
            with RecordWriter(temp_path, compression="zstd") as writer:
                for record in test_records:
                    writer.write_record(record)

            # Read back and verify
            read_records = []
            with RecordReader(temp_path) as reader:
                for record in reader:
                    read_records.append(record.to_bytes())

            # Verify all records match
            assert len(read_records) == len(test_records)
            for original, read_back in zip(test_records, read_records):
                assert original == read_back

        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_single_threaded_no_compression_explicit(self):
        """Test single-threaded writer with explicit 'none' compression."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            temp_path = temp.name

        try:
            test_records = [b"Record 1", b"Record 2"]

            # Write with explicit no compression
            with RecordWriter(temp_path, compression="none") as writer:
                for record in test_records:
                    writer.write_record(record)

            # Read back and verify
            read_records = []
            with RecordReader(temp_path) as reader:
                for record in reader:
                    read_records.append(record.to_bytes())

            assert len(read_records) == len(test_records)
            for original, read_back in zip(test_records, read_records):
                assert original == read_back

        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_multi_threaded_zstd_compression(self):
        """Test multi-threaded writer and reader with zstd compression."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Test records
            test_records = []
            for i in range(50):
                if i % 5 == 0:
                    # Add repetitive data that compresses well
                    test_records.append(f"Repetitive record {i}: " * 10)
                else:
                    test_records.append(f"Unique record {i}")

            # Write with multi-threaded zstd compression
            with MultiThreadedWriter.new_with_shards(
                dir_path=tmpdir,
                prefix="zstd_test",
                num_shards=2,
                worker_threads=2,
                compression="zstd"
            ) as writer:
                for record in test_records:
                    writer.write_record(record.encode('utf-8'))

            # Read back and verify
            read_records = []
            with MultiThreadedReader.new_with_shards(
                dir_path=tmpdir,
                prefix="zstd_test",
                num_shards=2,
                worker_threads=2,
            ) as reader:
                for record in reader:
                    read_records.append(record.to_bytes().decode('utf-8'))

            # Sort both lists since multi-threaded reading may return records in different order
            test_records.sort()
            read_records.sort()

            # Verify all records match
            assert len(read_records) == len(test_records)
            for original, read_back in zip(test_records, read_records):
                assert original == read_back

    def test_compression_effectiveness(self):
        """Test that zstd compression actually reduces file size for repetitive data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create highly repetitive data
            repetitive_record = b"COMPRESS_ME_" * 100  # 1200 bytes
            test_records = [repetitive_record] * 20    # 24KB total

            # Write without compression
            uncompressed_path = os.path.join(tmpdir, "uncompressed.disky")
            with RecordWriter(uncompressed_path, compression="none") as writer:
                for record in test_records:
                    writer.write_record(record)

            # Write with zstd compression
            compressed_path = os.path.join(tmpdir, "compressed.disky")
            with RecordWriter(compressed_path, compression="zstd") as writer:
                for record in test_records:
                    writer.write_record(record)

            # Compare file sizes
            uncompressed_size = os.path.getsize(uncompressed_path)
            compressed_size = os.path.getsize(compressed_path)

            # For highly repetitive data, compressed should be significantly smaller
            # Allow for some overhead from headers/metadata
            assert compressed_size < uncompressed_size * 0.5, \
                f"Expected significant compression, got {compressed_size} vs {uncompressed_size}"

            # Verify both files read the same data
            with RecordReader(uncompressed_path) as reader:
                uncompressed_records = [r.to_bytes() for r in reader]

            with RecordReader(compressed_path) as reader:
                compressed_records = [r.to_bytes() for r in reader]

            assert uncompressed_records == compressed_records

    def test_invalid_compression_type(self):
        """Test that invalid compression types raise appropriate errors."""
        with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
            temp_path = temp.name

        try:
            # Test invalid compression type
            with pytest.raises(Exception) as exc_info:
                RecordWriter(temp_path, compression="invalid_compression")

            assert "Unsupported compression type" in str(exc_info.value)

        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_multi_threaded_invalid_compression_type(self):
        """Test that invalid compression types raise appropriate errors in multi-threaded writer."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Test invalid compression type
            with pytest.raises(Exception) as exc_info:
                MultiThreadedWriter.new_with_shards(
                    dir_path=tmpdir,
                    prefix="invalid_test",
                    num_shards=2,
                    compression="invalid_compression"
                )

            assert "Unsupported compression type" in str(exc_info.value)


if __name__ == "__main__":
    pytest.main(["-v", __file__])
