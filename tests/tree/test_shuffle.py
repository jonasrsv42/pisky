"""Tests for shuffle tree composition."""

import os
import tempfile

import pytest

from pisky import RecordWriterConfig, RecordReaderConfig
from pisky.tree import ShuffleConfig, RoundRobinConfig, WeightedNodeConfig


class TestShuffleConfig:
    """Tests for ShuffleConfig tree composition."""

    def test_shuffle_preserves_all_records(self):
        """Test that shuffling preserves all records."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")

            expected = [f"record_{i}".encode() for i in range(20)]
            with RecordWriterConfig(path) as writer:
                for record in expected:
                    writer.write(record)

            config = ShuffleConfig(
                RecordReaderConfig(path),
                buffer_size=5,
                seed=42,
            )
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            # All records should be preserved
            assert sorted(records) == sorted(expected)

    def test_shuffle_changes_order(self):
        """Test that shuffling actually changes record order."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")

            original = [f"record_{i:03d}".encode() for i in range(50)]
            with RecordWriterConfig(path) as writer:
                for record in original:
                    writer.write(record)

            config = ShuffleConfig(
                RecordReaderConfig(path),
                buffer_size=10,
                seed=42,
            )
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            # Order should be different
            assert records != original
            # But content should be same
            assert sorted(records) == sorted(original)

    def test_shuffle_deterministic_with_seed(self):
        """Test that same seed produces same shuffle order."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")

            with RecordWriterConfig(path) as writer:
                for i in range(30):
                    writer.write(f"record_{i}".encode())

            # First shuffle with seed
            config1 = ShuffleConfig(
                RecordReaderConfig(path),
                buffer_size=5,
                seed=12345,
            )
            with config1 as reader:
                records1 = [bytes(r) for r in reader]

            # Second shuffle with same seed
            config2 = ShuffleConfig(
                RecordReaderConfig(path),
                buffer_size=5,
                seed=12345,
            )
            with config2 as reader:
                records2 = [bytes(r) for r in reader]

            assert records1 == records2

    def test_shuffle_different_seeds_different_order(self):
        """Test that different seeds produce different shuffle orders."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")

            with RecordWriterConfig(path) as writer:
                for i in range(30):
                    writer.write(f"record_{i}".encode())

            config1 = ShuffleConfig(
                RecordReaderConfig(path),
                buffer_size=5,
                seed=111,
            )
            with config1 as reader:
                records1 = [bytes(r) for r in reader]

            config2 = ShuffleConfig(
                RecordReaderConfig(path),
                buffer_size=5,
                seed=222,
            )
            with config2 as reader:
                records2 = [bytes(r) for r in reader]

            # Different seeds should produce different orders
            assert records1 != records2
            # But same content
            assert sorted(records1) == sorted(records2)

    def test_shuffle_nested_with_round_robin(self):
        """Test shuffle composed with round-robin."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path_a = os.path.join(tmpdir, "a.disky")
            path_b = os.path.join(tmpdir, "b.disky")

            with RecordWriterConfig(path_a) as writer:
                for i in range(5):
                    writer.write(f"a{i}".encode())

            with RecordWriterConfig(path_b) as writer:
                for i in range(5):
                    writer.write(f"b{i}".encode())

            # Shuffle the interleaved output
            config = ShuffleConfig(
                RoundRobinConfig([
                    RecordReaderConfig(path_a),
                    RecordReaderConfig(path_b),
                ]),
                buffer_size=3,
                seed=42,
            )
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            expected = [f"a{i}".encode() for i in range(5)] + [f"b{i}".encode() for i in range(5)]
            assert sorted(records) == sorted(expected)

    def test_shuffle_empty_input(self):
        """Test shuffle with empty input."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "empty.disky")

            with RecordWriterConfig(path) as writer:
                pass  # Empty file

            config = ShuffleConfig(
                RecordReaderConfig(path),
                buffer_size=10,
            )
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            assert records == []

    def test_shuffle_buffer_larger_than_input(self):
        """Test shuffle when buffer is larger than input."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "small.disky")

            expected = [b"one", b"two", b"three"]
            with RecordWriterConfig(path) as writer:
                for record in expected:
                    writer.write(record)

            config = ShuffleConfig(
                RecordReaderConfig(path),
                buffer_size=100,  # Much larger than input
                seed=42,
            )
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            assert sorted(records) == sorted(expected)

    def test_shuffle_exact_seed_ordering(self):
        """Test that shuffle with a seed produces exact deterministic ordering."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")

            # Write 10 records with known content
            with RecordWriterConfig(path) as writer:
                for i in range(10):
                    writer.write(f"r{i}".encode())

            # Read with seed=42 and buffer_size=3
            config = ShuffleConfig(
                RecordReaderConfig(path),
                buffer_size=3,
                seed=42,
            )
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            # With seed=42 and buffer_size=3, expect this exact order
            # (determined by reservoir shuffle algorithm)
            expected = [
                b"r1", b"r3", b"r4", b"r0", b"r5", b"r2", b"r8", b"r6", b"r9", b"r7",
            ]
            assert records == expected


class TestShuffleWeight:
    """Tests for ShuffleConfig weight behavior."""

    def test_shuffle_weight_passes_through_none(self):
        """Test that shuffle passes through None weight from unweighted child."""
        config = ShuffleConfig(RecordReaderConfig("dummy.disky"), buffer_size=10)
        assert config.weight is None

    def test_shuffle_weight_passes_through_weighted_child(self):
        """Test that shuffle passes through weight from weighted child."""
        config = ShuffleConfig(
            WeightedNodeConfig(RecordReaderConfig("dummy.disky"), 3.0),
            buffer_size=10,
        )
        assert config.weight == 3.0

    def test_shuffle_weight_passes_through_stacked_weights(self):
        """Test that shuffle passes through stacked weights (additive)."""
        config = ShuffleConfig(
            WeightedNodeConfig(
                WeightedNodeConfig(RecordReaderConfig("dummy.disky"), 2.0),
                3.0,
            ),
            buffer_size=10,
        )
        # Stacked weights are additive: 2.0 + 3.0 = 5.0
        assert config.weight == 5.0


class TestTreeReaderClose:
    """Tests for tree reader close behavior."""

    def test_shuffle_close_raises_error_on_subsequent_read(self):
        """Test that reading after close() raises an error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")

            with RecordWriterConfig(path) as writer:
                for i in range(5):
                    writer.write(f"r{i}".encode())

            config = ShuffleConfig(
                RecordReaderConfig(path),
                buffer_size=10,
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

    def test_round_robin_close_raises_error_on_subsequent_read(self):
        """Test that reading after close() raises an error for round-robin."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")

            with RecordWriterConfig(path) as writer:
                for i in range(5):
                    writer.write(f"r{i}".encode())

            config = RoundRobinConfig([RecordReaderConfig(path)])
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
