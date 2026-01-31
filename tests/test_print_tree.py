"""Test for named tree printing."""

from pisky import RecordReaderConfig
from pisky.tree import (
    AutoSamplingConfig,
    WeightedNodeConfig,
    LazyWeightedNodeConfig,
    NamedNodeConfig,
    ShuffleConfig,
    ThreadedConfig,
    RoundRobinConfig,
    SamplingConfig,
    named_tree,
)


def test_print_tree():
    """Print a semi-complicated tree structure."""

    # Build a realistic multi-language data pipeline
    config = AutoSamplingConfig([
        # English sources - high weight
        NamedNodeConfig("English", ThreadedConfig(
            WeightedNodeConfig(
                ShuffleConfig(
                    RoundRobinConfig([
                        RecordReaderConfig("en_wikipedia.disky"),
                        RecordReaderConfig("en_books.disky"),
                        RecordReaderConfig("en_news.disky"),
                    ]),
                    buffer_size=2000,
                    seed=123,
                ),
                5.0,
            ),
            buffer_size=64,
        )),

        # German sources - medium weight
        NamedNodeConfig("German", WeightedNodeConfig(
            RoundRobinConfig([
                NamedNodeConfig("DE-News", WeightedNodeConfig(
                    RecordReaderConfig("de_news.disky"),
                    2.0,
                )),
                NamedNodeConfig("DE-Books", WeightedNodeConfig(
                    ShuffleConfig(
                        RecordReaderConfig("de_books.disky"),
                        buffer_size=500,
                    ),
                    1.5,
                )),
            ]),
            1.0,  # Additional weight at German level
        )),

        # French sources - using lazy weighted
        NamedNodeConfig("French", LazyWeightedNodeConfig(
            lambda: (
                ShuffleConfig(
                    RecordReaderConfig("fr_corpus.disky"),
                    buffer_size=1000,
                    seed=456,
                ),
                2.0,
            )
        )),

        # Asian languages group
        NamedNodeConfig("Asian", RoundRobinConfig([
            NamedNodeConfig("Japanese", WeightedNodeConfig(
                RecordReaderConfig("ja_data.disky"),
                1.0,
            )),
            NamedNodeConfig("Chinese", WeightedNodeConfig(
                ThreadedConfig(
                    RecordReaderConfig("zh_data.disky"),
                    buffer_size=128,
                ),
                1.5,
            )),
            NamedNodeConfig("Korean", WeightedNodeConfig(
                RecordReaderConfig("ko_data.disky"),
                0.5,
            )),
        ])),
    ], seed=42)

    tree = named_tree(config, "MultilingualPipeline")
    print("\n" + "=" * 60)
    print("NAMED TREE OUTPUT:")
    print("=" * 60)
    print(tree)
    print("=" * 60)

    # Basic assertions
    assert tree.name == "MultilingualPipeline"
    assert tree.weight is not None
    assert len(tree.children) == 1  # AutoSamplingConfig

    auto_sampling = tree.children[0]
    assert auto_sampling.name == "AutoSamplingConfig"
    assert auto_sampling.metadata.get("seed") == 42


if __name__ == "__main__":
    test_print_tree()
