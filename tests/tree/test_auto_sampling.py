"""Tests for AutoSamplingConfig."""

import os
import tempfile

import pytest

from pisky import RecordWriterConfig, RecordReaderConfig
from pisky.shard.file_shards import FileShards
from pisky.shard.order import RandomRepeat
from pisky.shard.reader import SequentialConfig as ShardSequentialConfig
from pisky.tree import (
    AutoSamplingConfig,
    WeightedNodeConfig,
    LazyWeightedNodeConfig,
    ShuffleConfig,
    RoundRobinConfig,
)


class TestAutoSamplingConfig:
    """Tests for AutoSamplingConfig basic functionality."""

    def test_auto_sampling_basic(self):
        """Test basic auto sampling with weighted children."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path_a = os.path.join(tmpdir, "a.disky")
            path_b = os.path.join(tmpdir, "b.disky")

            with RecordWriterConfig(path_a) as writer:
                for i in range(10):
                    writer.write(f"a{i}".encode())

            with RecordWriterConfig(path_b) as writer:
                for i in range(10):
                    writer.write(f"b{i}".encode())

            config = AutoSamplingConfig(
                [
                    WeightedNodeConfig(RecordReaderConfig(path_a), 2.0),
                    WeightedNodeConfig(RecordReaderConfig(path_b), 1.0),
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

    def test_auto_sampling_single_source(self):
        """Test auto sampling with a single weighted source."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")

            expected = [f"r{i}".encode() for i in range(20)]
            with RecordWriterConfig(path) as writer:
                for record in expected:
                    writer.write(record)

            config = AutoSamplingConfig(
                [WeightedNodeConfig(RecordReaderConfig(path), 1.0)],
                seed=42,
            )
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            assert records == expected

    def test_auto_sampling_deterministic_with_seed(self):
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
            config1 = AutoSamplingConfig(
                [
                    WeightedNodeConfig(RecordReaderConfig(path_a), 2.0),
                    WeightedNodeConfig(RecordReaderConfig(path_b), 1.0),
                ],
                seed=12345,
            )
            with config1 as reader:
                records1 = [bytes(r) for r in reader]

            # Second run with same seed
            config2 = AutoSamplingConfig(
                [
                    WeightedNodeConfig(RecordReaderConfig(path_a), 2.0),
                    WeightedNodeConfig(RecordReaderConfig(path_b), 1.0),
                ],
                seed=12345,
            )
            with config2 as reader:
                records2 = [bytes(r) for r in reader]

            assert records1 == records2

    def test_auto_sampling_different_seeds_different_order(self):
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

            config1 = AutoSamplingConfig(
                [
                    WeightedNodeConfig(RecordReaderConfig(path_a), 1.0),
                    WeightedNodeConfig(RecordReaderConfig(path_b), 1.0),
                ],
                seed=111,
            )
            with config1 as reader:
                records1 = [bytes(r) for r in reader]

            config2 = AutoSamplingConfig(
                [
                    WeightedNodeConfig(RecordReaderConfig(path_a), 1.0),
                    WeightedNodeConfig(RecordReaderConfig(path_b), 1.0),
                ],
                seed=222,
            )
            with config2 as reader:
                records2 = [bytes(r) for r in reader]

            # Different seeds should produce different orders
            assert records1 != records2
            # But same content
            assert sorted(records1) == sorted(records2)


class TestAutoSamplingValidation:
    """Tests for AutoSamplingConfig validation."""

    def test_auto_sampling_empty_children_error(self):
        """Test that empty children raises an error."""
        with pytest.raises(ValueError, match="empty"):
            AutoSamplingConfig([], seed=42)

    def test_auto_sampling_unweighted_child_error(self):
        """Test that unweighted children raise error on enter."""
        config = AutoSamplingConfig(
            [RecordReaderConfig("dummy.disky")],
            seed=42,
        )
        with pytest.raises(ValueError, match="missing weights"):
            with config:
                pass

    def test_auto_sampling_mixed_weights_error_on_enter(self):
        """Test that mixed weighted/unweighted raises error with indices."""
        config = AutoSamplingConfig(
            [
                WeightedNodeConfig(RecordReaderConfig("a.disky"), 2.0),
                RecordReaderConfig("b.disky"),  # unweighted - index 1
                WeightedNodeConfig(RecordReaderConfig("c.disky"), 1.0),
                RecordReaderConfig("d.disky"),  # unweighted - index 3
            ],
            seed=42,
        )
        with pytest.raises(ValueError, match=r"\[1, 3\]"):
            with config:
                pass

    def test_auto_sampling_all_unweighted_error(self):
        """Test that all unweighted children raises error."""
        config = AutoSamplingConfig(
            [
                RecordReaderConfig("a.disky"),
                RecordReaderConfig("b.disky"),
            ],
            seed=42,
        )
        with pytest.raises(ValueError, match="missing weights"):
            with config:
                pass


class TestAutoSamplingWeight:
    """Tests for AutoSamplingConfig weight behavior."""

    def test_auto_sampling_weight_sum_of_children(self):
        """Test that weight is sum of children weights."""
        config = AutoSamplingConfig([
            WeightedNodeConfig(RecordReaderConfig("a.disky"), 2.0),
            WeightedNodeConfig(RecordReaderConfig("b.disky"), 3.0),
        ])
        assert config.weight == 5.0

    def test_auto_sampling_weight_all_unweighted_is_none(self):
        """Test that weight is None when all children are unweighted."""
        config = AutoSamplingConfig([
            RecordReaderConfig("a.disky"),
            RecordReaderConfig("b.disky"),
        ])
        assert config.weight is None

    def test_auto_sampling_weight_mixed_raises_error(self):
        """Test that mixed weighted/unweighted raises error on weight access."""
        config = AutoSamplingConfig([
            WeightedNodeConfig(RecordReaderConfig("a.disky"), 2.0),
            RecordReaderConfig("b.disky"),  # unweighted
        ])
        with pytest.raises(ValueError, match="missing weight"):
            _ = config.weight

    def test_auto_sampling_weight_nested(self):
        """Test weight with nested structures."""
        config = AutoSamplingConfig([
            WeightedNodeConfig(RecordReaderConfig("a.disky"), 2.0),
            RoundRobinConfig([
                WeightedNodeConfig(RecordReaderConfig("b.disky"), 1.0),
                WeightedNodeConfig(RecordReaderConfig("c.disky"), 3.0),
            ]),
        ])
        # 2.0 + (1.0 + 3.0) = 6.0
        assert config.weight == 6.0

    def test_auto_sampling_weight_stacked_weighted(self):
        """Test weight with stacked WeightedNodeConfigs."""
        config = AutoSamplingConfig([
            WeightedNodeConfig(
                WeightedNodeConfig(RecordReaderConfig("a.disky"), 1.0),
                2.0,
            ),
            WeightedNodeConfig(RecordReaderConfig("b.disky"), 3.0),
        ])
        # (2.0 + 1.0) + 3.0 = 6.0
        assert config.weight == 6.0


class TestAutoSamplingWithLazyWeighted:
    """Tests for AutoSamplingConfig with LazyWeightedNodeConfig."""

    def test_auto_sampling_with_lazy_weighted(self):
        """Test that lazy weighted nodes are resolved on enter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path_a = os.path.join(tmpdir, "a.disky")
            path_b = os.path.join(tmpdir, "b.disky")

            with RecordWriterConfig(path_a) as writer:
                for i in range(5):
                    writer.write(f"a{i}".encode())

            with RecordWriterConfig(path_b) as writer:
                for i in range(5):
                    writer.write(f"b{i}".encode())

            call_count = 0

            def factory_a():
                nonlocal call_count
                call_count += 1
                return (RecordReaderConfig(path_a), 2.0)

            def factory_b():
                nonlocal call_count
                call_count += 1
                return (RecordReaderConfig(path_b), 1.0)

            config = AutoSamplingConfig(
                [
                    LazyWeightedNodeConfig(factory_a),
                    LazyWeightedNodeConfig(factory_b),
                ],
                seed=42,
            )

            # Factories not called yet
            assert call_count == 0

            with config as reader:
                # Factories called during weight validation
                assert call_count == 2
                records = [bytes(r) for r in reader]

            expected = [f"a{i}".encode() for i in range(5)] + [
                f"b{i}".encode() for i in range(5)
            ]
            assert sorted(records) == sorted(expected)

    def test_auto_sampling_lazy_weight_resolution(self):
        """Test that lazy nodes resolve on weight access."""
        call_count = 0

        def factory():
            nonlocal call_count
            call_count += 1
            return (RecordReaderConfig("dummy.disky"), 3.0)

        config = AutoSamplingConfig([
            LazyWeightedNodeConfig(factory),
            WeightedNodeConfig(RecordReaderConfig("other.disky"), 2.0),
        ])

        # Not resolved yet
        assert call_count == 0

        # Access weight triggers resolution
        weight = config.weight
        assert call_count == 1
        assert weight == 5.0  # 3.0 + 2.0


class TestAutoSamplingNested:
    """Tests for AutoSamplingConfig with nested composition."""

    def test_auto_sampling_through_shuffle(self):
        """Test auto sampling with shuffle-wrapped weighted nodes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path_a = os.path.join(tmpdir, "a.disky")
            path_b = os.path.join(tmpdir, "b.disky")

            with RecordWriterConfig(path_a) as writer:
                for i in range(5):
                    writer.write(f"a{i}".encode())

            with RecordWriterConfig(path_b) as writer:
                for i in range(5):
                    writer.write(f"b{i}".encode())

            config = AutoSamplingConfig(
                [
                    ShuffleConfig(
                        WeightedNodeConfig(RecordReaderConfig(path_a), 2.0),
                        buffer_size=3,
                        seed=1,
                    ),
                    WeightedNodeConfig(RecordReaderConfig(path_b), 1.0),
                ],
                seed=42,
            )

            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            expected = [f"a{i}".encode() for i in range(5)] + [
                f"b{i}".encode() for i in range(5)
            ]
            assert sorted(records) == sorted(expected)

    def test_auto_sampling_weight_through_shuffle(self):
        """Test that weight passes through shuffle."""
        config = AutoSamplingConfig([
            ShuffleConfig(
                WeightedNodeConfig(RecordReaderConfig("a.disky"), 2.0),
                buffer_size=10,
            ),
            WeightedNodeConfig(RecordReaderConfig("b.disky"), 3.0),
        ])
        # 2.0 + 3.0 = 5.0
        assert config.weight == 5.0

    def test_auto_sampling_nested_round_robin(self):
        """Test auto sampling with round robin containing weighted children."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path_a = os.path.join(tmpdir, "a.disky")
            path_b = os.path.join(tmpdir, "b.disky")
            path_c = os.path.join(tmpdir, "c.disky")

            with RecordWriterConfig(path_a) as writer:
                for i in range(4):
                    writer.write(f"a{i}".encode())

            with RecordWriterConfig(path_b) as writer:
                for i in range(4):
                    writer.write(f"b{i}".encode())

            with RecordWriterConfig(path_c) as writer:
                for i in range(4):
                    writer.write(f"c{i}".encode())

            config = AutoSamplingConfig(
                [
                    WeightedNodeConfig(RecordReaderConfig(path_a), 1.0),
                    RoundRobinConfig([
                        WeightedNodeConfig(RecordReaderConfig(path_b), 2.0),
                        WeightedNodeConfig(RecordReaderConfig(path_c), 2.0),
                    ]),
                ],
                seed=42,
            )

            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            expected = (
                [f"a{i}".encode() for i in range(4)] +
                [f"b{i}".encode() for i in range(4)] +
                [f"c{i}".encode() for i in range(4)]
            )
            assert sorted(records) == sorted(expected)


class TestAutoSamplingStatisticalDistribution:
    """Tests for auto sampling weight distribution with infinite sources."""

    def test_auto_sampling_2_to_1_ratio_distribution(self):
        """Test that 2:1 weights produce approximately 2:1 sampling ratio."""
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

            config = AutoSamplingConfig(
                [
                    WeightedNodeConfig(
                        ShardSequentialConfig(RandomRepeat(shards_a, seed=1)),
                        2.0,
                    ),
                    WeightedNodeConfig(
                        ShardSequentialConfig(RandomRepeat(shards_b, seed=2)),
                        1.0,
                    ),
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

            # With 2:1 ratio, expect ~667 from a and ~333 from b
            expected_a = n_samples * (2.0 / 3.0)
            expected_b = n_samples * (1.0 / 3.0)

            assert abs(a_count - expected_a) < expected_a * 0.05
            assert abs(b_count - expected_b) < expected_b * 0.05

    def test_auto_sampling_equal_weights_distribution(self):
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

            config = AutoSamplingConfig(
                [
                    WeightedNodeConfig(
                        ShardSequentialConfig(RandomRepeat(shards_a, seed=1)),
                        1.0,
                    ),
                    WeightedNodeConfig(
                        ShardSequentialConfig(RandomRepeat(shards_b, seed=2)),
                        1.0,
                    ),
                    WeightedNodeConfig(
                        ShardSequentialConfig(RandomRepeat(shards_c, seed=3)),
                        1.0,
                    ),
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
            expected = n_samples / 3.0

            assert abs(a_count - expected) < expected * 0.05
            assert abs(b_count - expected) < expected * 0.05
            assert abs(c_count - expected) < expected * 0.05


class TestAutoSamplingReaderClose:
    """Tests for auto sampling reader close behavior."""

    def test_auto_sampling_close_raises_error_on_subsequent_read(self):
        """Test that reading after close() raises an error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")

            with RecordWriterConfig(path) as writer:
                for i in range(10):
                    writer.write(f"r{i}".encode())

            config = AutoSamplingConfig(
                [WeightedNodeConfig(RecordReaderConfig(path), 1.0)],
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


if __name__ == "__main__":
    pytest.main(["-v", __file__])
