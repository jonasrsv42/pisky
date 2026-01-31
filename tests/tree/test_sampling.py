"""Tests for sampling tree composition."""

import os
import tempfile

import pytest

from pisky import RecordWriterConfig, RecordReaderConfig
from pisky.shard.file_shards import FileShards
from pisky.shard.order import RandomRepeat
from pisky.shard.reader import SequentialConfig as ShardSequentialConfig
from pisky.tree import SamplingConfig, ShuffleConfig, RoundRobinConfig, WeightedNodeConfig


class TestSamplingConfig:
    """Tests for SamplingConfig tree composition."""

    def test_sampling_preserves_all_records(self):
        """Test that sampling preserves all records from all sources."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path_a = os.path.join(tmpdir, "a.disky")
            path_b = os.path.join(tmpdir, "b.disky")

            with RecordWriterConfig(path_a) as writer:
                for i in range(10):
                    writer.write(f"a{i}".encode())

            with RecordWriterConfig(path_b) as writer:
                for i in range(10):
                    writer.write(f"b{i}".encode())

            config = SamplingConfig(
                [
                    (RecordReaderConfig(path_a), 1.0),
                    (RecordReaderConfig(path_b), 1.0),
                ],
                seed=42,
            )
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            expected = [f"a{i}".encode() for i in range(10)] + [
                f"b{i}".encode() for i in range(10)
            ]
            assert sorted(records) == sorted(expected)

    def test_sampling_single_source(self):
        """Test sampling with a single source."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")

            expected = [f"r{i}".encode() for i in range(20)]
            with RecordWriterConfig(path) as writer:
                for record in expected:
                    writer.write(record)

            config = SamplingConfig(
                [(RecordReaderConfig(path), 1.0)],
                seed=42,
            )
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            assert records == expected

    def test_sampling_deterministic_with_seed(self):
        """Test that same seed produces same sampling order."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path_a = os.path.join(tmpdir, "a.disky")
            path_b = os.path.join(tmpdir, "b.disky")

            with RecordWriterConfig(path_a) as writer:
                for i in range(10):
                    writer.write(f"a{i}".encode())

            with RecordWriterConfig(path_b) as writer:
                for i in range(10):
                    writer.write(f"b{i}".encode())

            # First run with seed
            config1 = SamplingConfig(
                [
                    (RecordReaderConfig(path_a), 2.0),
                    (RecordReaderConfig(path_b), 1.0),
                ],
                seed=12345,
            )
            with config1 as reader:
                records1 = [bytes(r) for r in reader]

            # Second run with same seed
            config2 = SamplingConfig(
                [
                    (RecordReaderConfig(path_a), 2.0),
                    (RecordReaderConfig(path_b), 1.0),
                ],
                seed=12345,
            )
            with config2 as reader:
                records2 = [bytes(r) for r in reader]

            assert records1 == records2

    def test_sampling_different_seeds_different_order(self):
        """Test that different seeds produce different sampling orders."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path_a = os.path.join(tmpdir, "a.disky")
            path_b = os.path.join(tmpdir, "b.disky")

            with RecordWriterConfig(path_a) as writer:
                for i in range(20):
                    writer.write(f"a{i}".encode())

            with RecordWriterConfig(path_b) as writer:
                for i in range(20):
                    writer.write(f"b{i}".encode())

            config1 = SamplingConfig(
                [
                    (RecordReaderConfig(path_a), 1.0),
                    (RecordReaderConfig(path_b), 1.0),
                ],
                seed=111,
            )
            with config1 as reader:
                records1 = [bytes(r) for r in reader]

            config2 = SamplingConfig(
                [
                    (RecordReaderConfig(path_a), 1.0),
                    (RecordReaderConfig(path_b), 1.0),
                ],
                seed=222,
            )
            with config2 as reader:
                records2 = [bytes(r) for r in reader]

            # Different seeds should produce different orders
            assert records1 != records2
            # But same content
            assert sorted(records1) == sorted(records2)

    def test_sampling_empty_source_error(self):
        """Test that empty sources raises an error."""
        with pytest.raises(ValueError, match="empty"):
            SamplingConfig([], seed=42)

    def test_sampling_non_positive_weight_error(self):
        """Test that non-positive weights raise an error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")

            with RecordWriterConfig(path) as writer:
                writer.write(b"test")

            with pytest.raises(ValueError, match="positive"):
                SamplingConfig(
                    [(RecordReaderConfig(path), 0.0)],
                    seed=42,
                )

            with pytest.raises(ValueError, match="positive"):
                SamplingConfig(
                    [(RecordReaderConfig(path), -1.0)],
                    seed=42,
                )

    def test_sampling_nested_with_shuffle_exact_order(self):
        """Test sampling composed with shuffle produces exact deterministic order."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path_a = os.path.join(tmpdir, "a.disky")
            path_b = os.path.join(tmpdir, "b.disky")

            with RecordWriterConfig(path_a) as writer:
                for i in range(5):
                    writer.write(f"a{i}".encode())

            with RecordWriterConfig(path_b) as writer:
                for i in range(5):
                    writer.write(f"b{i}".encode())

            config = SamplingConfig(
                [
                    (ShuffleConfig(RecordReaderConfig(path_a), buffer_size=3, seed=1), 1.0),
                    (RecordReaderConfig(path_b), 1.0),
                ],
                seed=42,
            )
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            # Exact expected order with seed=42 for sampling and seed=1 for shuffle
            expected = [
                b"b0", b"b1", b"b2", b"a2", b"a1", b"a0", b"b3", b"b4", b"a3", b"a4",
            ]
            assert records == expected


    def test_sampling_exact_order(self):
        """Test that sampling produces exact deterministic order."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path_a = os.path.join(tmpdir, "a.disky")
            path_b = os.path.join(tmpdir, "b.disky")

            with RecordWriterConfig(path_a) as writer:
                for i in range(10):
                    writer.write(f"a{i}".encode())

            with RecordWriterConfig(path_b) as writer:
                for i in range(10):
                    writer.write(f"b{i}".encode())

            config = SamplingConfig(
                [
                    (RecordReaderConfig(path_a), 2.0),
                    (RecordReaderConfig(path_b), 1.0),
                ],
                seed=456,
            )
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            # Exact expected order with seed=456 and 2:1 weights
            # Good interspersion with max consecutive run of 2
            expected = [
                b"a0", b"a1", b"b0", b"a2", b"b1", b"a3", b"b2", b"a4", b"b3", b"b4",
                b"a5", b"b5", b"a6", b"a7", b"b6", b"b7", b"a8", b"b8", b"a9", b"b9",
            ]
            assert records == expected


class TestSamplingStatisticalDistribution:
    """Tests for sampling weight distribution with infinite sources."""

    def test_sampling_2_to_1_ratio_distribution(self):
        """Test that 2:1 weights produce approximately 2:1 sampling ratio."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path_a = os.path.join(tmpdir, "a.disky")
            path_b = os.path.join(tmpdir, "b.disky")

            # Write enough records for statistical significance
            with RecordWriterConfig(path_a) as writer:
                for i in range(500):
                    writer.write(f"a{i}".encode())

            with RecordWriterConfig(path_b) as writer:
                for i in range(500):
                    writer.write(f"b{i}".encode())

            # Use RandomRepeat for infinite iteration
            shards_a = FileShards.from_paths([path_a])
            shards_b = FileShards.from_paths([path_b])

            config = SamplingConfig(
                [
                    (ShardSequentialConfig(RandomRepeat(shards_a, seed=1)), 2.0),
                    (ShardSequentialConfig(RandomRepeat(shards_b, seed=2)), 1.0),
                ],
                seed=42,
            )

            # Sample 1000 records for statistical significance
            n_samples = 1000
            with config as reader:
                records = []
                for i, record in enumerate(reader):
                    if i >= n_samples:
                        break
                    records.append(bytes(record))

            a_count = sum(1 for r in records if r.startswith(b"a"))
            b_count = sum(1 for r in records if r.startswith(b"b"))

            # With 2:1 ratio, expect ~667 from a and ~333 from b
            expected_a = n_samples * (2.0 / 3.0)  # 667
            expected_b = n_samples * (1.0 / 3.0)  # 333

            assert abs(a_count - expected_a) < expected_a * 0.05, f"a_count={a_count}, expected ~{expected_a}"
            assert abs(b_count - expected_b) < expected_b * 0.05, f"b_count={b_count}, expected ~{expected_b}"

    def test_sampling_3_to_1_ratio_distribution(self):
        """Test that 3:1 weights produce approximately 3:1 sampling ratio."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path_a = os.path.join(tmpdir, "a.disky")
            path_b = os.path.join(tmpdir, "b.disky")

            with RecordWriterConfig(path_a) as writer:
                for i in range(500):
                    writer.write(f"a{i}".encode())

            with RecordWriterConfig(path_b) as writer:
                for i in range(500):
                    writer.write(f"b{i}".encode())

            shards_a = FileShards.from_paths([path_a])
            shards_b = FileShards.from_paths([path_b])

            config = SamplingConfig(
                [
                    (ShardSequentialConfig(RandomRepeat(shards_a, seed=1)), 3.0),
                    (ShardSequentialConfig(RandomRepeat(shards_b, seed=2)), 1.0),
                ],
                seed=42,
            )

            n_samples = 1000
            with config as reader:
                records = []
                for i, record in enumerate(reader):
                    if i >= n_samples:
                        break
                    records.append(bytes(record))

            a_count = sum(1 for r in records if r.startswith(b"a"))
            b_count = sum(1 for r in records if r.startswith(b"b"))

            # With 3:1 ratio, expect ~750 from a and ~250 from b
            expected_a = n_samples * (3.0 / 4.0)  # 750
            expected_b = n_samples * (1.0 / 4.0)  # 250

            assert abs(a_count - expected_a) < expected_a * 0.05, f"a_count={a_count}, expected ~{expected_a}"
            assert abs(b_count - expected_b) < expected_b * 0.05, f"b_count={b_count}, expected ~{expected_b}"

    def test_sampling_equal_weights_distribution(self):
        """Test that equal weights produce approximately equal sampling."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path_a = os.path.join(tmpdir, "a.disky")
            path_b = os.path.join(tmpdir, "b.disky")
            path_c = os.path.join(tmpdir, "c.disky")

            for path, prefix in [(path_a, "a"), (path_b, "b"), (path_c, "c")]:
                with RecordWriterConfig(path) as writer:
                    for i in range(500):
                        writer.write(f"{prefix}{i}".encode())

            shards_a = FileShards.from_paths([path_a])
            shards_b = FileShards.from_paths([path_b])
            shards_c = FileShards.from_paths([path_c])

            config = SamplingConfig(
                [
                    (ShardSequentialConfig(RandomRepeat(shards_a, seed=1)), 1.0),
                    (ShardSequentialConfig(RandomRepeat(shards_b, seed=2)), 1.0),
                    (ShardSequentialConfig(RandomRepeat(shards_c, seed=3)), 1.0),
                ],
                seed=42,
            )

            n_samples = 999  # Divisible by 3
            with config as reader:
                records = []
                for i, record in enumerate(reader):
                    if i >= n_samples:
                        break
                    records.append(bytes(record))

            a_count = sum(1 for r in records if r.startswith(b"a"))
            b_count = sum(1 for r in records if r.startswith(b"b"))
            c_count = sum(1 for r in records if r.startswith(b"c"))

            # With equal weights, expect ~333 from each
            expected = n_samples / 3.0  # 333

            assert abs(a_count - expected) < expected * 0.05, f"a_count={a_count}, expected ~{expected}"
            assert abs(b_count - expected) < expected * 0.05, f"b_count={b_count}, expected ~{expected}"
            assert abs(c_count - expected) < expected * 0.05, f"c_count={c_count}, expected ~{expected}"


class TestSamplingReaderClose:
    """Tests for sampling reader close behavior."""

    def test_sampling_close_raises_error_on_subsequent_read(self):
        """Test that reading after close() raises an error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")

            with RecordWriterConfig(path) as writer:
                for i in range(10):
                    writer.write(f"r{i}".encode())

            config = SamplingConfig(
                [(RecordReaderConfig(path), 1.0)],
                seed=42,
            )
            with config as reader:
                # Read one record successfully
                record = next(reader)
                assert record is not None

                # Close the reader explicitly
                reader.close()

                # Subsequent read should raise an error
                with pytest.raises(Exception) as exc_info:
                    next(reader)

                assert "closed" in str(exc_info.value).lower()


class TestSamplingWeight:
    """Tests for SamplingConfig weight behavior."""

    def test_sampling_weight_all_none(self):
        """Test that weight is None when all children are unweighted."""
        config = SamplingConfig([
            (RecordReaderConfig("a.disky"), 2.0),
            (RecordReaderConfig("b.disky"), 1.0),
        ])
        assert config.weight is None

    def test_sampling_weight_weighted_sum(self):
        """Test that weight is weighted sum: explicit_w * child_weight."""
        config = SamplingConfig([
            (WeightedNodeConfig(RecordReaderConfig("a.disky"), 2.0), 3.0),
            (WeightedNodeConfig(RecordReaderConfig("b.disky"), 1.0), 1.0),
        ])
        # 3.0 * 2.0 + 1.0 * 1.0 = 7.0
        assert config.weight == 7.0

    def test_sampling_weight_nested(self):
        """Test weight with nested weighted children."""
        config = SamplingConfig([
            (WeightedNodeConfig(RecordReaderConfig("a.disky"), 2.0), 2.0),
            (RoundRobinConfig([
                WeightedNodeConfig(RecordReaderConfig("b.disky"), 1.0),
                WeightedNodeConfig(RecordReaderConfig("c.disky"), 3.0),
            ]), 1.0),
        ])
        # 2.0 * 2.0 + 1.0 * (1.0 + 3.0) = 4.0 + 4.0 = 8.0
        assert config.weight == 8.0

    def test_sampling_weight_mixed_raises_error(self):
        """Test that mixed weighted/unweighted children raises error."""
        config = SamplingConfig([
            (WeightedNodeConfig(RecordReaderConfig("a.disky"), 2.0), 3.0),
            (RecordReaderConfig("b.disky"), 1.0),  # unweighted
        ])
        with pytest.raises(ValueError, match="missing weights"):
            _ = config.weight


if __name__ == "__main__":
    pytest.main(["-v", __file__])
