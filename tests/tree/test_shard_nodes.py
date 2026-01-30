"""Tests for shard configs as tree nodes."""

import os
import tempfile

import pytest

from pisky import RecordReaderConfig, RecordWriterConfig
from pisky.shard.file_shards import FileShards as ReaderFileShards
from pisky.shard.order import Sequential
from pisky.shard.reader import SequentialConfig as ShardSequentialConfig
from pisky.shard.reader import RoundRobinConfig as ShardRoundRobinConfig
from pisky.shard.writer import FileShards as WriterFileShards
from pisky.shard.writer import SequentialConfig as ShardWriterConfig
from pisky.tree import RoundRobinConfig


class TestShardNodesInTree:
    """Tests for using shard reader configs as tree nodes."""

    def test_shard_sequential_in_tree(self):
        """Test shard SequentialConfig as a tree node."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create 2 shards using max_shard_bytes to force rotation
            write_shards = WriterFileShards.from_pattern(tmpdir, "shard")
            with ShardWriterConfig(write_shards, max_shard_bytes=20) as writer:
                writer.write(b"s0")  # -> shard_0
                writer.write(b"s1")  # -> shard_0 (rotation happens after)
                writer.write(b"s2")  # -> shard_1
                writer.write(b"s3")  # -> shard_1

            # Create a single file
            single_path = os.path.join(tmpdir, "single.disky")
            with RecordWriterConfig(single_path) as writer:
                writer.write(b"a0")
                writer.write(b"a1")

            # Create reader shards from same pattern
            read_shards = ReaderFileShards.from_pattern(tmpdir, "shard")

            # Compose: interleave shard reader with single file reader
            shard_config = ShardSequentialConfig(Sequential(read_shards))
            single_config = RecordReaderConfig(single_path)

            config = RoundRobinConfig([shard_config, single_config])
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            # Should have all 6 records
            assert len(records) == 6
            # Shard records and single file records should both be present
            assert b"s0" in records
            assert b"s1" in records
            assert b"s2" in records
            assert b"s3" in records
            assert b"a0" in records
            assert b"a1" in records

    def test_shard_round_robin_in_tree(self):
        """Test shard RoundRobinConfig as a tree node."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create 2 shards using max_shard_bytes to force rotation
            write_shards = WriterFileShards.from_pattern(tmpdir, "shard")
            with ShardWriterConfig(write_shards, max_shard_bytes=20) as writer:
                writer.write(b"s0")
                writer.write(b"s1")
                writer.write(b"s2")
                writer.write(b"s3")

            # Create a single file
            single_path = os.path.join(tmpdir, "single.disky")
            with RecordWriterConfig(single_path) as writer:
                writer.write(b"a0")
                writer.write(b"a1")

            # Create reader shards from same pattern
            read_shards = ReaderFileShards.from_pattern(tmpdir, "shard")

            # Compose: interleave shard round-robin with single file
            shard_config = ShardRoundRobinConfig(Sequential(read_shards))
            single_config = RecordReaderConfig(single_path)

            config = RoundRobinConfig([shard_config, single_config])
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            # Should have all 6 records
            assert len(records) == 6
            assert b"s0" in records
            assert b"a0" in records

    def test_multiple_shard_configs_in_tree(self):
        """Test multiple shard configs composed in a tree."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create first set of shards (single shard)
            write_shards1 = WriterFileShards.from_pattern(tmpdir, "group1_")
            with ShardWriterConfig(write_shards1) as writer:
                writer.write(b"g1_0")
                writer.write(b"g1_1")

            # Create second set of shards (single shard)
            write_shards2 = WriterFileShards.from_pattern(tmpdir, "group2_")
            with ShardWriterConfig(write_shards2) as writer:
                writer.write(b"g2_0")
                writer.write(b"g2_1")

            # Create reader shards
            read_shards1 = ReaderFileShards.from_pattern(tmpdir, "group1_")
            read_shards2 = ReaderFileShards.from_pattern(tmpdir, "group2_")

            # Compose both shard groups
            config = RoundRobinConfig([
                ShardSequentialConfig(Sequential(read_shards1)),
                ShardSequentialConfig(Sequential(read_shards2)),
            ])
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            # Interleaved: g1_0, g2_0, g1_1, g2_1
            assert records == [b"g1_0", b"g2_0", b"g1_1", b"g2_1"]

    def test_nested_tree_with_shards(self):
        """Test nested tree composition with shard configs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create shards (single shard)
            write_shards = WriterFileShards.from_pattern(tmpdir, "shard")
            with ShardWriterConfig(write_shards) as writer:
                writer.write(b"s0")
                writer.write(b"s1")

            # Create two single files
            path_a = os.path.join(tmpdir, "a.disky")
            path_b = os.path.join(tmpdir, "b.disky")
            with RecordWriterConfig(path_a) as writer:
                writer.write(b"a0")
                writer.write(b"a1")
            with RecordWriterConfig(path_b) as writer:
                writer.write(b"b0")
                writer.write(b"b1")

            # Create reader shards
            read_shards = ReaderFileShards.from_pattern(tmpdir, "shard")

            # Nested: (shards, (a, b))
            config = RoundRobinConfig([
                ShardSequentialConfig(Sequential(read_shards)),
                RoundRobinConfig([
                    RecordReaderConfig(path_a),
                    RecordReaderConfig(path_b),
                ]),
            ])
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            # Inner RR produces: a0, b0, a1, b1
            # Outer RR interleaves shards with inner:
            # s0, a0, s1, b0, (shards exhausted), a1, b1
            assert records == [b"s0", b"a0", b"s1", b"b0", b"a1", b"b1"]

    def test_random_order_same_seed_same_shuffle(self):
        """Test that two RandomRepeat readers with same seed shuffle identically."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write group1 shards: g1_0 to g1_4
            for i in range(5):
                path = os.path.join(tmpdir, f"group1_{i}")
                with RecordWriterConfig(path) as w:
                    w.write(f"g1_{i}".encode())

            # Write group2 shards: g2_0 to g2_4
            for i in range(5):
                path = os.path.join(tmpdir, f"group2_{i}")
                with RecordWriterConfig(path) as w:
                    w.write(f"g2_{i}".encode())

            # Both with same seed=123 -> both shuffle to order [3,1,4,0,2]
            shards1 = ReaderFileShards.from_pattern(tmpdir, "group1_")
            shards2 = ReaderFileShards.from_pattern(tmpdir, "group2_")

            from pisky.shard.order import RandomRepeat

            config = RoundRobinConfig([
                ShardSequentialConfig(RandomRepeat(shards1, seed=123)),
                ShardSequentialConfig(RandomRepeat(shards2, seed=123)),
            ])

            records = []
            with config as reader:
                for i, record in enumerate(reader):
                    records.append(bytes(record))
                    if i >= 9:
                        break

            # Same seed -> same shuffle order -> paired interleaving
            expected = [
                b"g1_3", b"g2_3",
                b"g1_1", b"g2_1",
                b"g1_4", b"g2_4",
                b"g1_0", b"g2_0",
                b"g1_2", b"g2_2",
            ]
            assert records == expected

    def test_random_order_different_seeds_different_shuffle(self):
        """Test that two RandomRepeat readers with different seeds shuffle differently."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write group1 shards: g1_0 to g1_4
            for i in range(5):
                path = os.path.join(tmpdir, f"group1_{i}")
                with RecordWriterConfig(path) as w:
                    w.write(f"g1_{i}".encode())

            # Write group2 shards: g2_0 to g2_4
            for i in range(5):
                path = os.path.join(tmpdir, f"group2_{i}")
                with RecordWriterConfig(path) as w:
                    w.write(f"g2_{i}".encode())

            # seed=123 -> [3,1,4,0,2], seed=999 -> [2,1,4,3,0]
            shards1 = ReaderFileShards.from_pattern(tmpdir, "group1_")
            shards2 = ReaderFileShards.from_pattern(tmpdir, "group2_")

            from pisky.shard.order import RandomRepeat

            config = RoundRobinConfig([
                ShardSequentialConfig(RandomRepeat(shards1, seed=123)),
                ShardSequentialConfig(RandomRepeat(shards2, seed=999)),
            ])

            records = []
            with config as reader:
                for i, record in enumerate(reader):
                    records.append(bytes(record))
                    if i >= 9:
                        break

            # Different seeds -> different shuffle orders -> mixed interleaving
            expected = [
                b"g1_3", b"g2_2",
                b"g1_1", b"g2_1",
                b"g1_4", b"g2_4",
                b"g1_0", b"g2_3",
                b"g1_2", b"g2_0",
            ]
            assert records == expected


if __name__ == "__main__":
    pytest.main(["-v", __file__])
