"""Tests for WeightedNodeConfig."""

import os
import tempfile

import pytest

from pisky import RecordWriterConfig, RecordReaderConfig
from pisky.tree import WeightedNodeConfig, ShuffleConfig, RoundRobinConfig


class TestWeightedNodeConfig:
    """Tests for WeightedNodeConfig weight behavior."""

    def test_weighted_node_basic_weight(self):
        """Test that WeightedNodeConfig returns its weight."""
        config = WeightedNodeConfig(RecordReaderConfig("dummy.disky"), 3.0)
        assert config.weight == 3.0

    def test_weighted_node_stacked_additive(self):
        """Test that stacked WeightedNodeConfigs are additive."""
        config = WeightedNodeConfig(
            WeightedNodeConfig(RecordReaderConfig("dummy.disky"), 2.0),
            3.0,
        )
        # 3.0 + 2.0 = 5.0
        assert config.weight == 5.0

    def test_weighted_node_triple_stacked(self):
        """Test triple stacked WeightedNodeConfigs."""
        config = WeightedNodeConfig(
            WeightedNodeConfig(
                WeightedNodeConfig(RecordReaderConfig("dummy.disky"), 1.0),
                2.0,
            ),
            3.0,
        )
        # 3.0 + 2.0 + 1.0 = 6.0
        assert config.weight == 6.0

    def test_weighted_node_with_unweighted_child(self):
        """Test WeightedNodeConfig with unweighted child."""
        config = WeightedNodeConfig(RecordReaderConfig("dummy.disky"), 5.0)
        # Child has None weight, so just returns own weight
        assert config.weight == 5.0

    def test_weighted_node_non_positive_weight_error(self):
        """Test that non-positive weight raises error."""
        with pytest.raises(ValueError, match="positive"):
            WeightedNodeConfig(RecordReaderConfig("dummy.disky"), 0.0)

        with pytest.raises(ValueError, match="positive"):
            WeightedNodeConfig(RecordReaderConfig("dummy.disky"), -1.0)

    def test_weighted_node_missing_weight_error(self):
        """Test that missing weight raises error."""
        with pytest.raises(ValueError, match="required"):
            WeightedNodeConfig(RecordReaderConfig("dummy.disky"), None)


class TestWeightedNodeInTree:
    """Tests for WeightedNodeConfig in tree composition."""

    def test_weighted_node_through_shuffle(self):
        """Test WeightedNodeConfig weight passes through shuffle."""
        config = ShuffleConfig(
            WeightedNodeConfig(RecordReaderConfig("dummy.disky"), 4.0),
            buffer_size=10,
        )
        assert config.weight == 4.0

    def test_weighted_node_in_round_robin(self):
        """Test WeightedNodeConfig in RoundRobinConfig."""
        config = RoundRobinConfig([
            WeightedNodeConfig(RecordReaderConfig("a.disky"), 2.0),
            WeightedNodeConfig(RecordReaderConfig("b.disky"), 3.0),
        ])
        assert config.weight == 5.0

    def test_weighted_node_transparent_to_rust(self):
        """Test that WeightedNodeConfig is transparent when reading."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.disky")
            with RecordWriterConfig(path) as writer:
                writer.write(b"hello")
                writer.write(b"world")

            config = WeightedNodeConfig(RecordReaderConfig(path), 2.0)
            records = []
            with config as reader:
                for record in reader:
                    records.append(bytes(record))

            assert records == [b"hello", b"world"]


if __name__ == "__main__":
    pytest.main(["-v", __file__])
