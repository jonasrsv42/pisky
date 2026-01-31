"""Tests for tree-based reader composition."""

import os
import tempfile

import pytest

from pisky import RecordWriterConfig, RecordReaderConfig
from pisky.tree import RoundRobinConfig, WeightedNodeConfig


class TestRoundRobinConfig:
    """Tests for RoundRobinConfig tree composition."""

    def test_interleave_two_files(self):
        """Test round-robin interleaving of two files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path_a = os.path.join(tmpdir, "a.disky")
            path_b = os.path.join(tmpdir, "b.disky")

            # Write to file a
            with RecordWriterConfig(path_a) as writer:
                writer.write(b"a0")
                writer.write(b"a1")
                writer.write(b"a2")

            # Write to file b
            with RecordWriterConfig(path_b) as writer:
                writer.write(b"b0")
                writer.write(b"b1")

            # Read interleaved
            config = RoundRobinConfig([
                RecordReaderConfig(path_a),
                RecordReaderConfig(path_b),
            ])
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            # Should interleave: a0, b0, a1, b1, a2
            assert records == [b"a0", b"b0", b"a1", b"b1", b"a2"]

    def test_interleave_three_files(self):
        """Test round-robin with three files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = [os.path.join(tmpdir, f"{c}.disky") for c in "abc"]

            for i, path in enumerate(paths):
                with RecordWriterConfig(path) as writer:
                    writer.write(f"{chr(ord('a') + i)}0".encode())
                    writer.write(f"{chr(ord('a') + i)}1".encode())

            config = RoundRobinConfig([RecordReaderConfig(p) for p in paths])
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            # Should interleave: a0, b0, c0, a1, b1, c1
            assert records == [b"a0", b"b0", b"c0", b"a1", b"b1", b"c1"]

    def test_single_child(self):
        """Test round-robin with single child."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "single.disky")

            with RecordWriterConfig(path) as writer:
                writer.write(b"one")
                writer.write(b"two")

            config = RoundRobinConfig([RecordReaderConfig(path)])
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            assert records == [b"one", b"two"]

    def test_empty_children(self):
        """Test round-robin with no children."""
        config = RoundRobinConfig([])
        records = []
        with config as reader:
            for record in reader:
                records.append(bytes(record))

        assert records == []

    def test_nested_round_robin(self):
        """Test nested round-robin composition."""
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = [os.path.join(tmpdir, f"{c}.disky") for c in "abcd"]

            for i, path in enumerate(paths):
                with RecordWriterConfig(path) as writer:
                    writer.write(f"{chr(ord('a') + i)}0".encode())
                    writer.write(f"{chr(ord('a') + i)}1".encode())

            # Nest: ((a, b), (c, d))
            config = RoundRobinConfig([
                RoundRobinConfig([
                    RecordReaderConfig(paths[0]),
                    RecordReaderConfig(paths[1]),
                ]),
                RoundRobinConfig([
                    RecordReaderConfig(paths[2]),
                    RecordReaderConfig(paths[3]),
                ]),
            ])
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            # Inner round-robins produce: (a0,b0,a1,b1) and (c0,d0,c1,d1)
            # Outer interleaves: a0, c0, b0, d0, a1, c1, b1, d1
            assert records == [b"a0", b"c0", b"b0", b"d0", b"a1", b"c1", b"b1", b"d1"]

    def test_reusable_config(self):
        """Test that config can be used multiple times."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "reuse.disky")

            with RecordWriterConfig(path) as writer:
                writer.write(b"data")

            config = RoundRobinConfig([RecordReaderConfig(path)])

            # First use
            with config as reader:
                records1 = [bytes(r) for r in reader]

            # Second use
            with config as reader:
                records2 = [bytes(r) for r in reader]

            assert records1 == records2 == [b"data"]

    def test_uneven_file_lengths(self):
        """Test interleaving files with different record counts."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path_a = os.path.join(tmpdir, "a.disky")
            path_b = os.path.join(tmpdir, "b.disky")

            with RecordWriterConfig(path_a) as writer:
                for i in range(5):
                    writer.write(f"a{i}".encode())

            with RecordWriterConfig(path_b) as writer:
                writer.write(b"b0")

            config = RoundRobinConfig([
                RecordReaderConfig(path_a),
                RecordReaderConfig(path_b),
            ])
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            # b exhausts after first round, then a continues
            assert records == [b"a0", b"b0", b"a1", b"a2", b"a3", b"a4"]


class TestRoundRobinWeight:
    """Tests for RoundRobinConfig weight behavior."""

    def test_round_robin_weight_all_none(self):
        """Test that weight is None when all children are unweighted."""
        config = RoundRobinConfig([
            RecordReaderConfig("a.disky"),
            RecordReaderConfig("b.disky"),
        ])
        assert config.weight is None

    def test_round_robin_weight_sums_children(self):
        """Test that weight sums children weights."""
        config = RoundRobinConfig([
            WeightedNodeConfig(RecordReaderConfig("a.disky"), 2.0),
            WeightedNodeConfig(RecordReaderConfig("b.disky"), 3.0),
        ])
        assert config.weight == 5.0

    def test_round_robin_weight_sums_nested(self):
        """Test that weight sums nested weighted children."""
        config = RoundRobinConfig([
            WeightedNodeConfig(RecordReaderConfig("a.disky"), 2.0),
            RoundRobinConfig([
                WeightedNodeConfig(RecordReaderConfig("b.disky"), 1.0),
                WeightedNodeConfig(RecordReaderConfig("c.disky"), 4.0),
            ]),
        ])
        # 2.0 + (1.0 + 4.0) = 7.0
        assert config.weight == 7.0

    def test_round_robin_weight_mixed_raises_error(self):
        """Test that mixed weighted/unweighted children raises error."""
        config = RoundRobinConfig([
            WeightedNodeConfig(RecordReaderConfig("a.disky"), 2.0),
            RecordReaderConfig("b.disky"),  # unweighted
        ])
        with pytest.raises(ValueError, match="missing weights"):
            _ = config.weight


if __name__ == "__main__":
    pytest.main(["-v", __file__])
