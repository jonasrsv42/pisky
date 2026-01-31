"""Tests for threaded tree composition."""

import os
import tempfile

import pytest

from pisky import RecordWriterConfig, RecordReaderConfig
from pisky.tree import ThreadedConfig, ShuffleConfig, RoundRobinConfig, WeightedNodeConfig


class TestThreadedConfig:
    """Tests for ThreadedConfig tree composition."""

    def test_threaded_preserves_all_records(self):
        """Test that threaded reading preserves all records."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")

            expected = [f"record_{i}".encode() for i in range(50)]
            with RecordWriterConfig(path) as writer:
                for record in expected:
                    writer.write(record)

            config = ThreadedConfig(
                RecordReaderConfig(path),
                buffer_size=16,
            )
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            assert records == expected

    def test_threaded_maintains_order(self):
        """Test that threaded reading maintains record order."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")

            expected = [f"record_{i:03d}".encode() for i in range(100)]
            with RecordWriterConfig(path) as writer:
                for record in expected:
                    writer.write(record)

            config = ThreadedConfig(
                RecordReaderConfig(path),
                buffer_size=8,
            )
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            # Order should be preserved
            assert records == expected

    def test_threaded_with_small_buffer(self):
        """Test threaded with small buffer size."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")

            expected = [f"r{i}".encode() for i in range(100)]
            with RecordWriterConfig(path) as writer:
                for record in expected:
                    writer.write(record)

            config = ThreadedConfig(
                RecordReaderConfig(path),
                buffer_size=1,  # Minimal buffer
            )
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            assert records == expected

    def test_threaded_with_large_buffer(self):
        """Test threaded with large buffer size."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")

            expected = [f"r{i}".encode() for i in range(20)]
            with RecordWriterConfig(path) as writer:
                for record in expected:
                    writer.write(record)

            config = ThreadedConfig(
                RecordReaderConfig(path),
                buffer_size=1000,  # Much larger than data
            )
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            assert records == expected

    def test_threaded_empty_input(self):
        """Test threaded with empty input."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "empty.disky")

            with RecordWriterConfig(path) as writer:
                pass  # Empty file

            config = ThreadedConfig(
                RecordReaderConfig(path),
                buffer_size=10,
            )
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            assert records == []

    def test_threaded_wrapping_shuffle(self):
        """Test threaded wrapping a shuffle node."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")

            expected = [f"r{i}".encode() for i in range(30)]
            with RecordWriterConfig(path) as writer:
                for record in expected:
                    writer.write(record)

            config = ThreadedConfig(
                ShuffleConfig(
                    RecordReaderConfig(path),
                    buffer_size=5,
                    seed=42,
                ),
                buffer_size=16,
            )
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            # All records preserved, but shuffled
            assert sorted(records) == sorted(expected)
            # Order should be different (shuffled)
            assert records != expected

    def test_threaded_wrapping_round_robin(self):
        """Test threaded wrapping a round-robin node."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path_a = os.path.join(tmpdir, "a.disky")
            path_b = os.path.join(tmpdir, "b.disky")

            with RecordWriterConfig(path_a) as writer:
                for i in range(5):
                    writer.write(f"a{i}".encode())

            with RecordWriterConfig(path_b) as writer:
                for i in range(5):
                    writer.write(f"b{i}".encode())

            config = ThreadedConfig(
                RoundRobinConfig([
                    RecordReaderConfig(path_a),
                    RecordReaderConfig(path_b),
                ]),
                buffer_size=8,
            )
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            expected = [f"a{i}".encode() for i in range(5)] + [f"b{i}".encode() for i in range(5)]
            assert sorted(records) == sorted(expected)

    def test_nested_threaded_shuffle_round_robin(self):
        """Test deeply nested composition."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path_a = os.path.join(tmpdir, "a.disky")
            path_b = os.path.join(tmpdir, "b.disky")

            with RecordWriterConfig(path_a) as writer:
                for i in range(10):
                    writer.write(f"a{i}".encode())

            with RecordWriterConfig(path_b) as writer:
                for i in range(10):
                    writer.write(f"b{i}".encode())

            # Threaded(Shuffle(RoundRobin([a, b])))
            config = ThreadedConfig(
                ShuffleConfig(
                    RoundRobinConfig([
                        RecordReaderConfig(path_a),
                        RecordReaderConfig(path_b),
                    ]),
                    buffer_size=5,
                    seed=42,
                ),
                buffer_size=16,
            )
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            expected = [f"a{i}".encode() for i in range(10)] + [f"b{i}".encode() for i in range(10)]
            assert sorted(records) == sorted(expected)


class TestThreadedWeight:
    """Tests for ThreadedConfig weight behavior."""

    def test_threaded_weight_passes_through_none(self):
        """Test that threaded passes through None weight from unweighted child."""
        config = ThreadedConfig(RecordReaderConfig("dummy.disky"), buffer_size=10)
        assert config.weight is None

    def test_threaded_weight_passes_through_weighted_child(self):
        """Test that threaded passes through weight from weighted child."""
        config = ThreadedConfig(
            WeightedNodeConfig(RecordReaderConfig("dummy.disky"), 3.0),
            buffer_size=10,
        )
        assert config.weight == 3.0

    def test_threaded_weight_passes_through_stacked_weights(self):
        """Test that threaded passes through stacked weights (additive)."""
        config = ThreadedConfig(
            WeightedNodeConfig(
                WeightedNodeConfig(RecordReaderConfig("dummy.disky"), 2.0),
                3.0,
            ),
            buffer_size=10,
        )
        # Stacked weights are additive: 2.0 + 3.0 = 5.0
        assert config.weight == 5.0


class TestThreadedReaderClose:
    """Tests for threaded reader close behavior."""

    def test_threaded_close_raises_error_on_subsequent_read(self):
        """Test that reading after close() raises an error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")

            with RecordWriterConfig(path) as writer:
                for i in range(10):
                    writer.write(f"r{i}".encode())

            config = ThreadedConfig(
                RecordReaderConfig(path),
                buffer_size=16,
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
