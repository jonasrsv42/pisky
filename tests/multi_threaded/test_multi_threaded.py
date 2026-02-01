"""Tests for multi-threaded sharded reading and writing."""

import os
import tempfile
import pytest

from pisky.multi_threaded import reader, order, writer
from pisky.shard import writer as shard_writer
from pisky.tree import RoundRobinConfig, SamplingConfig, ShuffleConfig, tree_from_bytes


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


class TestMultiThreadedSerialization:
    """Tests for multi-threaded reader serialization."""

    def test_sequential_order_serialize_roundtrip(self):
        """Test MultiThreadedConfig with Sequential order serialize/deserialize."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write data using shard writer
            sink = shard_writer.FileShards.from_pattern(tmpdir, "shard")
            expected = [f"record_{i}".encode() for i in range(30)]

            with shard_writer.SequentialConfig(sink, max_shard_bytes=100) as w:
                for record in expected:
                    w.write(record)

            # Create config and serialize
            shards = reader.FileShards.from_pattern(tmpdir, "shard")
            seq = order.Sequential(shards)
            config = reader.MultiThreadedConfig(seq, num_parallel=2)

            serialized = config.serialize()
            assert isinstance(serialized, bytes)

            # Deserialize and read
            with tree_from_bytes(serialized) as r:
                records = [bytes(rec) for rec in r]

            assert sorted(records) == sorted(expected)

    def test_random_order_serialize_roundtrip(self):
        """Test MultiThreadedConfig with RandomRepeat order serialize/deserialize."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write data using shard writer
            sink = shard_writer.FileShards.from_pattern(tmpdir, "shard")
            expected = [f"record_{i}".encode() for i in range(20)]

            with shard_writer.SequentialConfig(sink, max_shard_bytes=100) as w:
                for record in expected:
                    w.write(record)

            # Create config with random order and seed for reproducibility
            shards = reader.FileShards.from_pattern(tmpdir, "shard")
            rand = order.RandomRepeat(shards, seed=42)
            config = reader.MultiThreadedConfig(rand, num_parallel=2)

            serialized = config.serialize()
            assert isinstance(serialized, bytes)

            # Deserialize and read (limited since infinite)
            with tree_from_bytes(serialized) as r:
                records = []
                for i, rec in enumerate(r):
                    records.append(bytes(rec))
                    if i >= 39:  # Take 40 records
                        break

            assert len(records) == 40
            # All records should be from our expected set
            for record in records:
                assert record in expected

    def test_serialize_with_all_options(self):
        """Test serialization with all config options set."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write data
            sink = shard_writer.FileShards.from_pattern(tmpdir, "shard")
            expected = [f"record_{i}".encode() for i in range(20)]

            with shard_writer.SequentialConfig(sink, max_shard_bytes=100) as w:
                for record in expected:
                    w.write(record)

            # Create config with all options
            shards = reader.FileShards.from_pattern(tmpdir, "shard")
            seq = order.Sequential(shards)
            config = reader.MultiThreadedConfig(
                seq,
                num_parallel=3,
                worker_threads=2,
                queue_size_mb=16,
            )

            serialized = config.serialize()
            assert isinstance(serialized, bytes)

            # Deserialize and read
            with tree_from_bytes(serialized) as r:
                records = [bytes(rec) for rec in r]

            assert sorted(records) == sorted(expected)

    def test_serialize_deterministic(self):
        """Test that serialization is deterministic with same seed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write data
            sink = shard_writer.FileShards.from_pattern(tmpdir, "shard")
            for i in range(10):
                with shard_writer.SequentialConfig(sink, max_shard_bytes=100) as w:
                    w.write(f"record_{i}".encode())

            shards = reader.FileShards.from_pattern(tmpdir, "shard")

            # Create two configs with same seed
            rand1 = order.RandomRepeat(shards, seed=12345)
            config1 = reader.MultiThreadedConfig(rand1, num_parallel=2)

            rand2 = order.RandomRepeat(shards, seed=12345)
            config2 = reader.MultiThreadedConfig(rand2, num_parallel=2)

            serialized1 = config1.serialize()
            serialized2 = config2.serialize()

            assert serialized1 == serialized2

    def test_nested_tree_with_multithreaded(self):
        """Test MultiThreadedConfig nested in a deeper tree structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create two shard directories
            dir_a = os.path.join(tmpdir, "a")
            dir_b = os.path.join(tmpdir, "b")
            os.makedirs(dir_a)
            os.makedirs(dir_b)

            # Write data to dir_a
            sink_a = shard_writer.FileShards.from_pattern(dir_a, "shard")
            expected_a = [f"a_{i}".encode() for i in range(20)]
            with shard_writer.SequentialConfig(sink_a, max_shard_bytes=50) as w:
                for record in expected_a:
                    w.write(record)

            # Write data to dir_b
            sink_b = shard_writer.FileShards.from_pattern(dir_b, "shard")
            expected_b = [f"b_{i}".encode() for i in range(20)]
            with shard_writer.SequentialConfig(sink_b, max_shard_bytes=50) as w:
                for record in expected_b:
                    w.write(record)

            # Create multi-threaded readers for each shard set
            shards_a = reader.FileShards.from_pattern(dir_a, "shard")
            shards_b = reader.FileShards.from_pattern(dir_b, "shard")

            mt_a = reader.MultiThreadedConfig(order.Sequential(shards_a), num_parallel=2)
            mt_b = reader.MultiThreadedConfig(order.Sequential(shards_b), num_parallel=2)

            # Compose into a deeper tree: Shuffle(RoundRobin([mt_a, mt_b]))
            tree = ShuffleConfig(
                RoundRobinConfig([mt_a, mt_b]),
                buffer_size=50,
                seed=42,
            )

            serialized = tree.serialize()
            assert isinstance(serialized, bytes)

            # Deserialize and read
            with tree_from_bytes(serialized) as r:
                records = [bytes(rec) for rec in r]

            # All records from both sources should be present
            assert len(records) == 40
            assert set(records) == set(expected_a + expected_b)

    def test_sampling_with_multithreaded_sources(self):
        """Test SamplingConfig with MultiThreadedConfig sources."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create two shard directories
            dir_a = os.path.join(tmpdir, "a")
            dir_b = os.path.join(tmpdir, "b")
            os.makedirs(dir_a)
            os.makedirs(dir_b)

            # Write data to dir_a
            sink_a = shard_writer.FileShards.from_pattern(dir_a, "shard")
            expected_a = [f"a_{i}".encode() for i in range(15)]
            with shard_writer.SequentialConfig(sink_a, max_shard_bytes=50) as w:
                for record in expected_a:
                    w.write(record)

            # Write data to dir_b
            sink_b = shard_writer.FileShards.from_pattern(dir_b, "shard")
            expected_b = [f"b_{i}".encode() for i in range(15)]
            with shard_writer.SequentialConfig(sink_b, max_shard_bytes=50) as w:
                for record in expected_b:
                    w.write(record)

            # Create multi-threaded readers with random order (infinite)
            shards_a = reader.FileShards.from_pattern(dir_a, "shard")
            shards_b = reader.FileShards.from_pattern(dir_b, "shard")

            mt_a = reader.MultiThreadedConfig(
                order.RandomRepeat(shards_a, seed=42), num_parallel=2
            )
            mt_b = reader.MultiThreadedConfig(
                order.RandomRepeat(shards_b, seed=43), num_parallel=2
            )

            # Compose into SamplingConfig with 50/50 weights
            tree = SamplingConfig(
                [(mt_a, 1.0), (mt_b, 1.0)],
                seed=44,
            )

            serialized = tree.serialize()
            assert isinstance(serialized, bytes)

            # Deserialize and read (limited since infinite)
            with tree_from_bytes(serialized) as r:
                records = []
                a_count = 0
                b_count = 0
                for i, rec in enumerate(r):
                    record = bytes(rec)
                    records.append(record)
                    if record.startswith(b"a_"):
                        a_count += 1
                    elif record.startswith(b"b_"):
                        b_count += 1
                    if i >= 99:  # Take 100 records
                        break

            assert len(records) == 100
            # With 50/50 sampling, both sources should be well represented
            assert a_count > 30, f"Expected >30 from a, got {a_count}"
            assert b_count > 30, f"Expected >30 from b, got {b_count}"


if __name__ == "__main__":
    pytest.main(["-v", __file__])
