"""Tests for metadata() method on all NodeConfig implementations."""

import os
import tempfile

import pytest

from pisky import RecordReaderConfig, RecordWriterConfig
from pisky.shard.file_shards import FileShards
from pisky.shard.order import Sequential, RandomRepeat
from pisky.shard.reader import SequentialConfig, RoundRobinConfig as ShardRoundRobinConfig
from pisky.multi_threaded.reader import MultiThreadedConfig
from pisky.tree import (
    ShuffleConfig,
    ThreadedConfig,
    WeightedNodeConfig,
    NamedNodeConfig,
    RoundRobinConfig,
    SamplingConfig,
    AutoSamplingConfig,
)
from pisky.tree.lazy_weighted_node import LazyWeightedNodeConfig


class TestLeafNodeMetadata:
    """Tests for metadata on leaf nodes."""

    def test_record_reader_config_metadata(self):
        """RecordReaderConfig returns type and path."""
        config = RecordReaderConfig("test/path.disky")
        metadata = config.metadata()

        assert len(metadata) == 1
        assert metadata[0]["type"] == "RecordReaderConfig"
        assert metadata[0]["path"] == "test/path.disky"

    def test_sequential_config_metadata(self):
        """SequentialConfig returns type and path."""
        shards = FileShards.from_pattern("/data/train", "shard")
        config = SequentialConfig(Sequential(shards))
        metadata = config.metadata()

        assert len(metadata) == 1
        assert metadata[0]["type"] == "SequentialConfig"
        assert metadata[0]["path"] == "/data/train/shard"

    def test_shard_round_robin_config_metadata(self):
        """Shard RoundRobinConfig returns type and path."""
        shards = FileShards.from_prefix("/data/train/shard")
        config = ShardRoundRobinConfig(Sequential(shards))
        metadata = config.metadata()

        assert len(metadata) == 1
        assert metadata[0]["type"] == "RoundRobinConfig"
        assert metadata[0]["path"] == "/data/train/shard"

    def test_multi_threaded_config_metadata(self):
        """MultiThreadedConfig returns type and path."""
        shards = FileShards.from_paths(["/data/shard_0", "/data/shard_1"])
        config = MultiThreadedConfig(Sequential(shards))
        metadata = config.metadata()

        assert len(metadata) == 1
        assert metadata[0]["type"] == "MultiThreadedConfig"
        assert metadata[0]["path"] == "/data/shard_0, /data/shard_1"


class TestPassThroughNodeMetadata:
    """Tests for metadata on pass-through nodes (delegate to child)."""

    def test_shuffle_config_passes_through(self):
        """ShuffleConfig passes through child's metadata."""
        child = RecordReaderConfig("test.disky")
        config = ShuffleConfig(child, buffer_size=10)

        assert config.metadata() == child.metadata()

    def test_threaded_config_passes_through(self):
        """ThreadedConfig passes through child's metadata."""
        child = RecordReaderConfig("test.disky")
        config = ThreadedConfig(child, buffer_size=10)

        assert config.metadata() == child.metadata()

    def test_weighted_node_config_passes_through(self):
        """WeightedNodeConfig passes through child's metadata."""
        child = RecordReaderConfig("test.disky")
        config = WeightedNodeConfig(child, weight=2.0)

        assert config.metadata() == child.metadata()

    def test_named_node_config_passes_through(self):
        """NamedNodeConfig passes through child's metadata."""
        child = RecordReaderConfig("test.disky")
        config = NamedNodeConfig("MyName", child)

        assert config.metadata() == child.metadata()

    def test_lazy_weighted_node_config_passes_through(self):
        """LazyWeightedNodeConfig passes through child's metadata."""
        def factory():
            return (RecordReaderConfig("test.disky"), 2.0)

        config = LazyWeightedNodeConfig(factory)
        child = RecordReaderConfig("test.disky")

        assert config.metadata() == child.metadata()

    def test_nested_pass_through(self):
        """Nested pass-through nodes all delegate correctly."""
        child = RecordReaderConfig("deep.disky")
        config = ShuffleConfig(
            ThreadedConfig(
                WeightedNodeConfig(
                    NamedNodeConfig("Inner", child),
                    weight=1.0,
                ),
                buffer_size=10,
            ),
            buffer_size=100,
        )

        assert config.metadata() == child.metadata()


class TestMultiChildNodeMetadata:
    """Tests for metadata on multi-child nodes (collect from children)."""

    def test_round_robin_config_collects_children(self):
        """RoundRobinConfig collects metadata from all children."""
        child1 = RecordReaderConfig("a.disky")
        child2 = RecordReaderConfig("b.disky")
        config = RoundRobinConfig([child1, child2])

        metadata = config.metadata()
        assert len(metadata) == 2
        assert metadata[0] == child1.metadata()[0]
        assert metadata[1] == child2.metadata()[0]

    def test_sampling_config_collects_children(self):
        """SamplingConfig collects metadata from all children."""
        child1 = RecordReaderConfig("a.disky")
        child2 = RecordReaderConfig("b.disky")
        config = SamplingConfig([(child1, 1.0), (child2, 2.0)])

        metadata = config.metadata()
        assert len(metadata) == 2
        assert metadata[0] == child1.metadata()[0]
        assert metadata[1] == child2.metadata()[0]

    def test_auto_sampling_config_collects_children(self):
        """AutoSamplingConfig collects metadata from all children."""
        child1 = WeightedNodeConfig(RecordReaderConfig("a.disky"), 1.0)
        child2 = WeightedNodeConfig(RecordReaderConfig("b.disky"), 2.0)
        config = AutoSamplingConfig([child1, child2])

        metadata = config.metadata()
        assert len(metadata) == 2
        # WeightedNodeConfig passes through, so we get RecordReaderConfig metadata
        assert metadata[0]["type"] == "RecordReaderConfig"
        assert metadata[0]["path"] == "a.disky"
        assert metadata[1]["type"] == "RecordReaderConfig"
        assert metadata[1]["path"] == "b.disky"

    def test_nested_multi_child(self):
        """Nested multi-child nodes collect all leaves."""
        inner = RoundRobinConfig([
            RecordReaderConfig("a.disky"),
            RecordReaderConfig("b.disky"),
        ])
        outer = RoundRobinConfig([
            inner,
            RecordReaderConfig("c.disky"),
        ])

        metadata = outer.metadata()
        assert len(metadata) == 3
        paths = [m["path"] for m in metadata]
        assert paths == ["a.disky", "b.disky", "c.disky"]


class TestComplexTreeMetadata:
    """Tests for metadata on complex tree structures."""

    def test_complex_tree(self):
        """Test metadata collection from a complex tree."""
        tree = ShuffleConfig(
            AutoSamplingConfig([
                WeightedNodeConfig(
                    ThreadedConfig(RecordReaderConfig("en.disky")),
                    weight=2.0,
                ),
                NamedNodeConfig(
                    "German",
                    WeightedNodeConfig(
                        RoundRobinConfig([
                            RecordReaderConfig("de_news.disky"),
                            RecordReaderConfig("de_books.disky"),
                        ]),
                        weight=1.0,
                    ),
                ),
            ]),
            buffer_size=1000,
        )

        metadata = tree.metadata()
        assert len(metadata) == 3
        paths = [m["path"] for m in metadata]
        assert "en.disky" in paths
        assert "de_news.disky" in paths
        assert "de_books.disky" in paths

    def test_all_types_present(self):
        """Test that all metadata entries have type field."""
        tree = SamplingConfig([
            (RecordReaderConfig("a.disky"), 1.0),
            (RecordReaderConfig("b.disky"), 1.0),
        ])

        metadata = tree.metadata()
        for m in metadata:
            assert "type" in m


if __name__ == "__main__":
    pytest.main(["-v", __file__])
