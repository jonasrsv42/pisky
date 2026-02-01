"""Tests for tree config serialization."""

import os
import tempfile

import pytest

from pisky import RecordReaderConfig, RecordWriterConfig
from pisky.tree import (
    RoundRobinConfig,
    SamplingConfig,
    ShuffleConfig,
    ThreadedConfig,
    tree_from_bytes,
)


class TestSerialization:
    """Tests for serialize() and tree_from_bytes() round-trip."""

    def test_record_reader_serialize_roundtrip(self):
        """Test RecordReaderConfig serialize/deserialize round-trip."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")
            with RecordWriterConfig(path) as writer:
                writer.write(b"hello")
                writer.write(b"world")

            config = RecordReaderConfig(path)
            serialized = config.serialize()

            # Deserialize and read
            with tree_from_bytes(serialized) as reader:
                records = [bytes(r) for r in reader]

            assert records == [b"hello", b"world"]

    def test_round_robin_serialize_roundtrip(self):
        """Test RoundRobinConfig serialize/deserialize round-trip."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path_a = os.path.join(tmpdir, "a.disky")
            path_b = os.path.join(tmpdir, "b.disky")

            with RecordWriterConfig(path_a) as writer:
                writer.write(b"a0")
                writer.write(b"a1")

            with RecordWriterConfig(path_b) as writer:
                writer.write(b"b0")
                writer.write(b"b1")

            config = RoundRobinConfig([
                RecordReaderConfig(path_a),
                RecordReaderConfig(path_b),
            ])
            serialized = config.serialize()

            # Deserialize and read
            with tree_from_bytes(serialized) as reader:
                records = [bytes(r) for r in reader]

            assert records == [b"a0", b"b0", b"a1", b"b1"]

    def test_sampling_serialize_roundtrip(self):
        """Test SamplingConfig serialize/deserialize round-trip."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path_a = os.path.join(tmpdir, "a.disky")
            path_b = os.path.join(tmpdir, "b.disky")

            with RecordWriterConfig(path_a) as writer:
                for i in range(10):
                    writer.write(f"a{i}".encode())

            with RecordWriterConfig(path_b) as writer:
                for i in range(10):
                    writer.write(f"b{i}".encode())

            config = SamplingConfig([
                (RecordReaderConfig(path_a), 1.0),
                (RecordReaderConfig(path_b), 1.0),
            ], seed=42)
            serialized = config.serialize()

            # Deserialize and read
            with tree_from_bytes(serialized) as reader:
                records = [bytes(r) for r in reader]

            # Same seed should produce same order
            assert len(records) == 20
            assert b"a0" in records
            assert b"b0" in records

    def test_shuffle_serialize_roundtrip(self):
        """Test ShuffleConfig serialize/deserialize round-trip."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")

            with RecordWriterConfig(path) as writer:
                for i in range(10):
                    writer.write(f"r{i}".encode())

            config = ShuffleConfig(
                RecordReaderConfig(path),
                buffer_size=100,
                seed=42,
            )
            serialized = config.serialize()

            # Deserialize and read
            with tree_from_bytes(serialized) as reader:
                records = [bytes(r) for r in reader]

            # All records present
            assert len(records) == 10
            assert set(records) == {f"r{i}".encode() for i in range(10)}

    def test_threaded_serialize_roundtrip(self):
        """Test ThreadedConfig serialize/deserialize round-trip."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")

            with RecordWriterConfig(path) as writer:
                writer.write(b"hello")
                writer.write(b"world")

            config = ThreadedConfig(
                RecordReaderConfig(path),
                buffer_size=64,
            )
            serialized = config.serialize()

            # Deserialize and read
            with tree_from_bytes(serialized) as reader:
                records = [bytes(r) for r in reader]

            assert records == [b"hello", b"world"]

    def test_nested_tree_serialize_roundtrip(self):
        """Test nested tree config serialize/deserialize round-trip."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path_a = os.path.join(tmpdir, "a.disky")
            path_b = os.path.join(tmpdir, "b.disky")
            path_c = os.path.join(tmpdir, "c.disky")

            with RecordWriterConfig(path_a) as writer:
                writer.write(b"a0")
                writer.write(b"a1")

            with RecordWriterConfig(path_b) as writer:
                writer.write(b"b0")
                writer.write(b"b1")

            with RecordWriterConfig(path_c) as writer:
                writer.write(b"c0")
                writer.write(b"c1")

            # Nested: Shuffle(RoundRobin([a, b])), c
            config = RoundRobinConfig([
                ShuffleConfig(
                    RoundRobinConfig([
                        RecordReaderConfig(path_a),
                        RecordReaderConfig(path_b),
                    ]),
                    buffer_size=100,
                    seed=42,
                ),
                RecordReaderConfig(path_c),
            ])
            serialized = config.serialize()

            # Deserialize and read
            with tree_from_bytes(serialized) as reader:
                records = [bytes(r) for r in reader]

            # All records present
            assert len(records) == 6
            assert b"a0" in records
            assert b"b0" in records
            assert b"c0" in records

    def test_serialize_is_bytes(self):
        """Test that serialize() returns bytes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")
            with RecordWriterConfig(path) as writer:
                writer.write(b"test")

            config = RecordReaderConfig(path)
            serialized = config.serialize()

            assert isinstance(serialized, bytes)

    def test_tree_from_bytes_invalid_data(self):
        """Test tree_from_bytes with invalid data raises error."""
        with pytest.raises(ValueError):
            tree_from_bytes(b"not valid json")

    def test_tree_from_bytes_invalid_utf8(self):
        """Test tree_from_bytes with invalid UTF-8 raises error."""
        with pytest.raises(ValueError):
            tree_from_bytes(b"\xff\xfe")


if __name__ == "__main__":
    pytest.main(["-v", __file__])
