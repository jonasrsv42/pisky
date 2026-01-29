"""Tests for sharded reading and writing."""

import os
import tempfile
import pytest

from pisky.shard import reader, order, writer


class TestShardWriter:
    """Tests for shard writer."""

    def test_write_single_shard(self):
        """Test writing to a single shard."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sink = writer.FileShards.from_pattern(tmpdir, "shard")

            with writer.SequentialConfig(sink) as w:
                w.write(b"hello")
                w.write(b"world")

            # Check shard file was created
            assert os.path.exists(os.path.join(tmpdir, "shard_0"))

    def test_write_multiple_shards(self):
        """Test shard rotation based on max_shard_bytes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sink = writer.FileShards.from_pattern(tmpdir, "shard")

            # Small max_shard_bytes to force rotation
            with writer.SequentialConfig(sink, max_shard_bytes=50) as w:
                for i in range(10):
                    w.write(f"record_{i:04d}".encode())

            # Check multiple shards were created
            shard_files = [f for f in os.listdir(tmpdir) if f.startswith("shard_")]
            assert len(shard_files) > 1


class TestShardReader:
    """Tests for shard reader."""

    def test_sequential_reader_sequential_order(self):
        """Test sequential reader with sequential order."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write data
            sink = writer.FileShards.from_pattern(tmpdir, "shard")
            expected = [f"record_{i}".encode() for i in range(100)]

            with writer.SequentialConfig(sink, max_shard_bytes=100) as w:
                for record in expected:
                    w.write(record)

            # Read back with sequential order
            shards = reader.FileShards.from_pattern(tmpdir, "shard")
            seq = order.Sequential(shards)

            records = []
            with reader.SequentialConfig(seq) as r:
                for record in r:
                    records.append(bytes(record))

            assert records == expected

    def test_roundrobin_reader_sequential_order(self):
        """Test round-robin reader with sequential order."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write data to multiple shards
            sink = writer.FileShards.from_pattern(tmpdir, "shard")
            expected = [f"record_{i}".encode() for i in range(20)]

            with writer.SequentialConfig(sink, max_shard_bytes=50) as w:
                for record in expected:
                    w.write(record)

            # Read back with round-robin
            shards = reader.FileShards.from_pattern(tmpdir, "shard")
            seq = order.Sequential(shards)

            records = []
            with reader.RoundRobinConfig(seq) as r:
                for record in r:
                    records.append(bytes(record))

            # Round-robin may return records in different order, but same content
            assert sorted(records) == sorted(expected)

    def test_roundrobin_reader_with_max_active(self):
        """Test round-robin reader with max_active limiting concurrent shards."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write data to multiple shards
            sink = writer.FileShards.from_pattern(tmpdir, "shard")
            expected = [f"record_{i}".encode() for i in range(30)]

            with writer.SequentialConfig(sink, max_shard_bytes=50) as w:
                for record in expected:
                    w.write(record)

            # Verify we have multiple shards
            shard_files = [f for f in os.listdir(tmpdir) if f.startswith("shard_")]
            assert len(shard_files) > 2, "Need multiple shards for this test"

            # Read back with round-robin and max_active=2
            shards = reader.FileShards.from_pattern(tmpdir, "shard")
            seq = order.Sequential(shards)

            records = []
            with reader.RoundRobinConfig(seq, max_active=2) as r:
                for record in r:
                    records.append(bytes(record))

            # Should still get all records
            assert sorted(records) == sorted(expected)

    def test_roundrobin_random_order_with_max_active(self):
        """Test round-robin reader with random order and max_active (required for infinite)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write data to multiple shards
            sink = writer.FileShards.from_pattern(tmpdir, "shard")
            expected = [f"record_{i}".encode() for i in range(20)]

            with writer.SequentialConfig(sink, max_shard_bytes=50) as w:
                for record in expected:
                    w.write(record)

            # Read with random order - must use max_active since RandomRepeat is infinite
            shards = reader.FileShards.from_pattern(tmpdir, "shard")
            rand = order.RandomRepeat(shards)

            records = []
            with reader.RoundRobinConfig(rand, max_active=2) as r:
                for i, record in enumerate(r):
                    records.append(bytes(record))
                    if i >= 49:  # Take first 50 records
                        break

            # Should have 50 records (with repeats from infinite iteration)
            assert len(records) == 50
            # All records should be from our expected set
            for record in records:
                assert record in expected

    def test_sequential_reader_random_order(self):
        """Test sequential reader with random repeating order (finite read)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write data
            sink = writer.FileShards.from_pattern(tmpdir, "shard")
            expected = [f"record_{i}".encode() for i in range(50)]

            with writer.SequentialConfig(sink, max_shard_bytes=100) as w:
                for record in expected:
                    w.write(record)

            # Read with random order (take limited number since infinite)
            shards = reader.FileShards.from_pattern(tmpdir, "shard")
            rand = order.RandomRepeat(shards)

            records = []
            with reader.SequentialConfig(rand) as r:
                for i, record in enumerate(r):
                    records.append(bytes(record))
                    if i >= 99:  # Take first 100 records
                        break

            # Should have at least 100 records (may have duplicates due to repeat)
            assert len(records) == 100
            # All records should be from our expected set
            for record in records:
                assert record in expected


class TestFileShards:
    """Tests for FileShards construction."""

    def test_from_pattern(self):
        """Test FileShards.from_pattern."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create some shard files
            sink = writer.FileShards.from_pattern(tmpdir, "data")
            with writer.SequentialConfig(sink) as w:
                w.write(b"test")

            shards = reader.FileShards.from_pattern(tmpdir, "data")
            assert shards is not None

    def test_from_prefix(self):
        """Test FileShards.from_prefix."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create some shard files
            sink = writer.FileShards.from_prefix(os.path.join(tmpdir, "data"))
            with writer.SequentialConfig(sink) as w:
                w.write(b"test")

            shards = reader.FileShards.from_prefix(os.path.join(tmpdir, "data"))
            assert shards is not None

    def test_from_paths(self):
        """Test FileShards.from_paths."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create some shard files
            sink = writer.FileShards.from_pattern(tmpdir, "shard")
            with writer.SequentialConfig(sink, max_shard_bytes=20) as w:
                for i in range(5):
                    w.write(f"record_{i}".encode())

            # Get the paths manually
            paths = [
                os.path.join(tmpdir, f)
                for f in sorted(os.listdir(tmpdir))
                if f.startswith("shard_")
            ]

            shards = reader.FileShards.from_paths(paths)
            seq = order.Sequential(shards)

            records = []
            with reader.SequentialConfig(seq) as r:
                for record in r:
                    records.append(bytes(record))

            assert len(records) == 5


class TestWriterAppend:
    """Tests for writer append mode."""

    def test_append_continues_numbering(self):
        """Test that append mode continues shard numbering."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # First write
            sink1 = writer.FileShards.from_pattern(tmpdir, "shard")
            with writer.SequentialConfig(sink1) as w:
                w.write(b"first")

            assert os.path.exists(os.path.join(tmpdir, "shard_0"))

            # Append write
            sink2 = writer.FileShards.from_pattern(tmpdir, "shard", append=True)
            with writer.SequentialConfig(sink2) as w:
                w.write(b"second")

            assert os.path.exists(os.path.join(tmpdir, "shard_1"))

            # Read all
            shards = reader.FileShards.from_pattern(tmpdir, "shard")
            seq = order.Sequential(shards)

            records = []
            with reader.SequentialConfig(seq) as r:
                for record in r:
                    records.append(bytes(record))

            assert records == [b"first", b"second"]


if __name__ == "__main__":
    pytest.main(["-v", __file__])
