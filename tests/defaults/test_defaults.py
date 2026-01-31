"""Tests for pisky.defaults convenience functions."""

import tempfile
from pathlib import Path

import pytest

from pisky.defaults import (
    read_shards,
    write_shards,
    count_shards,
    read_shards_parallel,
    write_shards_parallel,
    count_shards_parallel,
)


class TestSingleThreadedDefaults:
    """Tests for single-threaded read_shards and write_shards."""

    def test_write_and_read_shards(self):
        """Test basic write and read cycle."""
        with tempfile.TemporaryDirectory() as tmpdir:
            records = [f"record_{i}".encode() for i in range(100)]

            # Write
            with write_shards(tmpdir) as writer:
                for record in records:
                    writer.write(record)

            # Read back
            read_records = []
            with read_shards(tmpdir) as reader:
                for record in reader:
                    read_records.append(record.to_bytes())

            assert read_records == records

    def test_write_shards_with_max_bytes(self):
        """Test that max_shard_bytes creates multiple shards."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write 100 records of ~100 bytes each with small max_shard_bytes
            records = [b"x" * 100 for _ in range(100)]

            with write_shards(tmpdir, max_shard_bytes=1000) as writer:
                for record in records:
                    writer.write(record)

            # Should have created multiple shard files
            shard_files = list(Path(tmpdir).glob("shard_*"))
            assert len(shard_files) > 1

            # Read back and verify
            read_records = []
            with read_shards(tmpdir) as reader:
                for record in reader:
                    read_records.append(record.to_bytes())

            assert len(read_records) == len(records)

    def test_write_shards_no_compression(self):
        """Test writing without compression."""
        with tempfile.TemporaryDirectory() as tmpdir:
            records = [b"test_record" for _ in range(10)]

            with write_shards(tmpdir, compression=None) as writer:
                for record in records:
                    writer.write(record)

            read_records = []
            with read_shards(tmpdir) as reader:
                for record in reader:
                    read_records.append(record.to_bytes())

            assert len(read_records) == len(records)

    def test_write_shards_append(self):
        """Test append mode."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # First write
            with write_shards(tmpdir) as writer:
                writer.write(b"first")

            # Append
            with write_shards(tmpdir, append=True) as writer:
                writer.write(b"second")

            # Read back
            read_records = []
            with read_shards(tmpdir) as reader:
                for record in reader:
                    read_records.append(record.to_bytes())

            assert read_records == [b"first", b"second"]

    def test_read_shards_empty_dir_raises(self):
        """Test that reading from empty dir raises ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError, match="No shard files"):
                with read_shards(tmpdir) as reader:
                    list(reader)

    def test_count_shards(self):
        """Test counting records across shards."""
        with tempfile.TemporaryDirectory() as tmpdir:
            records = [f"record_{i}".encode() for i in range(100)]

            with write_shards(tmpdir, max_shard_bytes=1000) as writer:
                for record in records:
                    writer.write(record)

            count = count_shards(tmpdir)
            assert count == 100

    def test_custom_pattern(self):
        """Test custom shard pattern prefix."""
        with tempfile.TemporaryDirectory() as tmpdir:
            records = [b"record" for _ in range(10)]

            with write_shards(tmpdir, pattern="data") as writer:
                for record in records:
                    writer.write(record)

            # Should have created files with "data_" prefix
            shard_files = list(Path(tmpdir).glob("data_*"))
            assert len(shard_files) >= 1

            # Read back with same pattern
            read_records = []
            with read_shards(tmpdir, pattern="data") as reader:
                for record in reader:
                    read_records.append(record.to_bytes())

            assert len(read_records) == len(records)


class TestMultiThreadedDefaults:
    """Tests for multi-threaded read_shards_parallel and write_shards_parallel."""

    def test_write_and_read_parallel(self):
        """Test basic parallel write and read cycle."""
        with tempfile.TemporaryDirectory() as tmpdir:
            records = [f"record_{i}".encode() for i in range(100)]

            # Write with multiple shards
            with write_shards_parallel(tmpdir, num_shards=2) as writer:
                for record in records:
                    writer.write(record)

            # Read back
            read_records = []
            with read_shards_parallel(tmpdir, num_parallel=2) as reader:
                for record in reader:
                    read_records.append(record.to_bytes())

            # Order may differ due to parallel writing, but content should match
            assert sorted(read_records) == sorted(records)

    def test_read_parallel_shuffle(self):
        """Test shuffled reading returns records (order not deterministic due to threads)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            records = [f"record_{i}".encode() for i in range(50)]

            with write_shards_parallel(tmpdir, num_shards=2) as writer:
                for record in records:
                    writer.write(record)

            # Read more than total records to verify infinite iteration works
            read_records = []
            with read_shards_parallel(tmpdir, shuffle=True, seed=42) as reader:
                for i, record in enumerate(reader):
                    if i >= 100:  # Read 2x the records
                        break
                    read_records.append(record.to_bytes())

            assert len(read_records) == 100
            # All original records should appear (possibly multiple times)
            assert set(records).issubset(set(read_records))

    def test_read_parallel_shuffle_requires_seed(self):
        """Test that shuffle=True requires seed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create some data first
            with write_shards_parallel(tmpdir) as writer:
                writer.write(b"test")

            with pytest.raises(ValueError, match="seed is required"):
                with read_shards_parallel(tmpdir, shuffle=True) as reader:
                    list(reader)

    def test_write_parallel_append(self):
        """Test append mode with parallel writer."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # First write
            with write_shards_parallel(tmpdir, num_shards=1) as writer:
                writer.write(b"first")

            # Append
            with write_shards_parallel(tmpdir, num_shards=1, append=True) as writer:
                writer.write(b"second")

            # Read back
            read_records = []
            with read_shards_parallel(tmpdir) as reader:
                for record in reader:
                    read_records.append(record.to_bytes())

            assert sorted(read_records) == [b"first", b"second"]

    def test_count_shards_parallel(self):
        """Test counting records across shards (parallel)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            records = [f"record_{i}".encode() for i in range(100)]

            with write_shards_parallel(tmpdir, num_shards=2) as writer:
                for record in records:
                    writer.write(record)

            count = count_shards_parallel(tmpdir)
            assert count == 100

    def test_write_parallel_no_compression(self):
        """Test parallel writing without compression."""
        with tempfile.TemporaryDirectory() as tmpdir:
            records = [b"test_record" for _ in range(10)]

            with write_shards_parallel(tmpdir, compression=None) as writer:
                for record in records:
                    writer.write(record)

            read_records = []
            with read_shards_parallel(tmpdir) as reader:
                for record in reader:
                    read_records.append(record.to_bytes())

            assert len(read_records) == len(records)
