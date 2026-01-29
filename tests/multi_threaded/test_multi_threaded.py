"""Tests for multi-threaded sharded reading and writing."""

import os
import tempfile
import pytest

from pisky.multi_threaded import reader, order, writer
from pisky.shard import writer as shard_writer


class TestMultiThreadedWriter:
    """Tests for multi-threaded shard writer."""

    def test_write_single_shard(self):
        """Test writing to shards with multi-threaded writer."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sink = writer.FileShards.from_pattern(tmpdir, "shard")

            with writer.MultiThreadedConfig(sink, num_shards=2) as w:
                w.write(b"hello")
                w.write(b"world")

            # Check shard file was created
            shard_files = [f for f in os.listdir(tmpdir) if f.startswith("shard_")]
            assert len(shard_files) >= 1

    def test_write_many_records(self):
        """Test writing many records with multi-threaded writer."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sink = writer.FileShards.from_pattern(tmpdir, "shard")
            expected = [f"record_{i}".encode() for i in range(100)]

            with writer.MultiThreadedConfig(sink, num_shards=2, worker_threads=2) as w:
                for record in expected:
                    w.write(record)

            # Verify with multi-threaded reader
            shards = reader.FileShards.from_pattern(tmpdir, "shard")
            seq = order.Sequential(shards)

            records = []
            with reader.MultiThreadedConfig(seq, num_parallel=2) as r:
                for record in r:
                    records.append(bytes(record))

            assert sorted(records) == sorted(expected)


class TestMultiThreadedReader:
    """Tests for multi-threaded shard reader."""

    def test_sequential_order(self):
        """Test multi-threaded reader with sequential order."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write data using shard writer
            sink = shard_writer.FileShards.from_pattern(tmpdir, "shard")
            expected = [f"record_{i}".encode() for i in range(50)]

            with shard_writer.SequentialConfig(sink, max_shard_bytes=100) as w:
                for record in expected:
                    w.write(record)

            # Read back with multi-threaded reader
            shards = reader.FileShards.from_pattern(tmpdir, "shard")
            seq = order.Sequential(shards)

            records = []
            with reader.MultiThreadedConfig(seq, num_parallel=2) as r:
                for record in r:
                    records.append(bytes(record))

            # Multi-threaded reader may return records in different order
            assert sorted(records) == sorted(expected)

    def test_random_order(self):
        """Test multi-threaded reader with random repeating order."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write data using shard writer
            sink = shard_writer.FileShards.from_pattern(tmpdir, "shard")
            expected = [f"record_{i}".encode() for i in range(30)]

            with shard_writer.SequentialConfig(sink, max_shard_bytes=100) as w:
                for record in expected:
                    w.write(record)

            # Read with random order (take limited number since infinite)
            shards = reader.FileShards.from_pattern(tmpdir, "shard")
            rand = order.RandomRepeat(shards)

            records = []
            with reader.MultiThreadedConfig(rand, num_parallel=2) as r:
                for i, record in enumerate(r):
                    records.append(bytes(record))
                    if i >= 59:  # Take first 60 records
                        break

            # Should have 60 records (with repeats)
            assert len(records) == 60
            # All records should be from our expected set
            for record in records:
                assert record in expected


class TestMultiThreadedRoundTrip:
    """Tests for round-trip write/read with multi-threaded."""

    def test_round_trip(self):
        """Test full round-trip with multi-threaded writer and reader."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sink = writer.FileShards.from_pattern(tmpdir, "shard")
            expected = [f"record_{i:04d}".encode() for i in range(200)]

            # Write with multi-threaded
            with writer.MultiThreadedConfig(
                sink, num_shards=4, worker_threads=2, enable_auto_sharding=True
            ) as w:
                for record in expected:
                    w.write(record)

            # Read with multi-threaded
            shards = reader.FileShards.from_pattern(tmpdir, "shard")
            seq = order.Sequential(shards)

            records = []
            with reader.MultiThreadedConfig(seq, num_parallel=4, worker_threads=2) as r:
                for record in r:
                    records.append(bytes(record))

            assert sorted(records) == sorted(expected)


if __name__ == "__main__":
    pytest.main(["-v", __file__])
