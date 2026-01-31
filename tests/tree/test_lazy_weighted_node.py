"""Tests for LazyWeightedNodeConfig."""

import os
import tempfile

import pytest

from pisky import RecordWriterConfig, RecordReaderConfig
from pisky.tree import LazyWeightedNodeConfig, ShuffleConfig


class TestLazyWeightedNodeConfig:
    """Tests for LazyWeightedNodeConfig weight behavior."""

    def test_lazy_weighted_node_resolves_on_weight_access(self):
        """Test that lazy node resolves when weight is accessed."""
        call_count = 0

        def factory():
            nonlocal call_count
            call_count += 1
            return (RecordReaderConfig("dummy.disky"), 3.0)

        config = LazyWeightedNodeConfig(factory)
        assert call_count == 0  # Not called yet

        weight = config.weight
        assert call_count == 1  # Called now
        assert weight == 3.0

        # Second access doesn't call again
        weight2 = config.weight
        assert call_count == 1
        assert weight2 == 3.0

    def test_lazy_weighted_node_stacked_additive(self):
        """Test lazy node stacked with regular WeightedNodeConfig."""
        from pisky.tree import WeightedNodeConfig

        def factory():
            return (WeightedNodeConfig(RecordReaderConfig("dummy.disky"), 2.0), 3.0)

        config = LazyWeightedNodeConfig(factory)
        # 3.0 + 2.0 = 5.0
        assert config.weight == 5.0

    def test_lazy_weighted_node_non_positive_weight_error(self):
        """Test that non-positive weight from factory raises error."""
        def factory():
            return (RecordReaderConfig("dummy.disky"), 0.0)

        config = LazyWeightedNodeConfig(factory)
        with pytest.raises(ValueError, match="positive"):
            _ = config.weight

    def test_lazy_weighted_node_negative_weight_error(self):
        """Test that negative weight from factory raises error."""
        def factory():
            return (RecordReaderConfig("dummy.disky"), -1.0)

        config = LazyWeightedNodeConfig(factory)
        with pytest.raises(ValueError, match="positive"):
            _ = config.weight


class TestLazyWeightedNodeInTree:
    """Tests for LazyWeightedNodeConfig in tree composition."""

    def test_lazy_weighted_node_through_shuffle(self):
        """Test lazy node weight passes through shuffle."""
        def factory():
            return (RecordReaderConfig("dummy.disky"), 4.0)

        config = ShuffleConfig(
            LazyWeightedNodeConfig(factory),
            buffer_size=10,
        )
        assert config.weight == 4.0

    def test_lazy_weighted_node_transparent_to_rust(self):
        """Test that lazy node is transparent when reading."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")
            with RecordWriterConfig(path) as writer:
                writer.write(b"hello")
                writer.write(b"world")

            def factory():
                return (RecordReaderConfig(path), 2.0)

            config = LazyWeightedNodeConfig(factory)
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            assert records == [b"hello", b"world"]

    def test_lazy_weighted_node_dynamic_path(self):
        """Test lazy node with dynamically computed path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")
            with RecordWriterConfig(path) as writer:
                writer.write(b"dynamic")

            # Simulate dynamic path discovery
            discovered_path = None

            def discover_path():
                nonlocal discovered_path
                discovered_path = path
                return path

            def factory():
                return (RecordReaderConfig(discover_path()), 1.5)

            config = LazyWeightedNodeConfig(factory)

            # Path not discovered yet
            assert discovered_path is None

            # Reading triggers resolution
            with config as reader:
                records = [bytes(r) for r in reader]

            assert discovered_path == path
            assert records == [b"dynamic"]


if __name__ == "__main__":
    pytest.main(["-v", __file__])
