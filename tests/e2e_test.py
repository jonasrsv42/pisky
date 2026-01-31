"""End-to-end test with realistic multi-language data pipeline."""

import tempfile
from pathlib import Path

import pytest

from pisky.shard.file_shards import FileShards as ReaderFileShards
from pisky.shard.order import RandomRepeat
from pisky.shard.reader import SequentialConfig as ShardSequentialConfig
from pisky.shard.writer import FileShards as WriterFileShards
from pisky.shard.writer import SequentialConfig as ShardWriterConfig
from pisky.tree import (
    AutoSamplingConfig,
    NamedNodeConfig,
    ShuffleConfig,
    ThreadedConfig,
    WeightedNodeConfig,
    named_tree,
    terse_tree,
)


def create_language_shards(
    base_dir: Path,
    lang: str,
    num_shards: int,
    records_per_shard: int,
) -> Path:
    """Create sharded data for a language."""
    lang_dir = base_dir / lang
    lang_dir.mkdir(parents=True, exist_ok=True)

    # Write each shard separately
    for shard_idx in range(num_shards):
        shard_prefix = lang_dir / f"shard_{shard_idx:03d}"
        shards = WriterFileShards.from_prefix(str(shard_prefix))
        with ShardWriterConfig(shards) as writer:
            for i in range(records_per_shard):
                # Each record contains the language code for verification
                record = f"{lang}:{shard_idx}:{i}".encode()
                writer.write(record)

    return lang_dir


def build_language_node(lang_dir: Path, lang: str, weight: float, seed: int):
    """Build a single language node: ShardReader (sequential, random order) → Shuffle → Weighted."""
    shards = ReaderFileShards.from_pattern(str(lang_dir), "shard")
    order = RandomRepeat(shards, seed=seed)
    return NamedNodeConfig(
        lang.upper(),
        WeightedNodeConfig(
            ShuffleConfig(
                ShardSequentialConfig(order),
                buffer_size=100,
                seed=seed + 1000,
            ),
            weight,
        ),
    )


def build_language_group(
    base_dir: Path,
    languages: list[tuple[str, float, int]],  # (lang_code, weight, seed)
    group_name: str,
    group_seed: int,
    num_shards: int = 3,
    records_per_shard: int = 500,
) -> tuple[NamedNodeConfig, float]:
    """Build a language group: multiple languages → AutoSampling → Thread.

    Returns the config and total weight of the group.
    """
    nodes = []
    total_weight = 0.0

    for lang, weight, seed in languages:
        lang_dir = create_language_shards(base_dir, lang, num_shards, records_per_shard)
        nodes.append(build_language_node(lang_dir, lang, weight, seed))
        total_weight += weight

    config = NamedNodeConfig(
        group_name,
        ThreadedConfig(
            AutoSamplingConfig(nodes, seed=group_seed),
            buffer_size=64,
        ),
    )

    return config, total_weight


class TestE2EMultiLanguagePipeline:
    """End-to-end test for realistic multi-language sampling pipeline."""

    def test_multi_language_pipeline_proportions(self):
        """Test that sampling proportions match expected weights within 5%."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)

            # Group 1: Nordic + Germanic (4 languages)
            # (lang, weight, seed) - sv=2.0, no=1.5, en=5.0, de=3.0 → total=11.5
            group1_languages = [
                ("sv", 2.0, 100),
                ("no", 1.5, 200),
                ("en", 5.0, 300),
                ("de", 3.0, 400),
            ]
            group1, group1_weight = build_language_group(
                base_dir, group1_languages, "Nordic-Germanic", group_seed=1000
            )

            # Group 2: Romance + Asian (3 languages)
            # fr=2.5, kr=1.0, zh=2.0 → total=5.5
            group2_languages = [
                ("fr", 2.5, 500),
                ("kr", 1.0, 600),
                ("zh", 2.0, 700),
            ]
            group2, group2_weight = build_language_group(
                base_dir, group2_languages, "Romance-Asian", group_seed=2000
            )

            # Group 3: Swiss (1 language)
            # ch=1.5 → total=1.5
            group3_languages = [
                ("ch", 1.5, 800),
            ]
            group3, group3_weight = build_language_group(
                base_dir, group3_languages, "Swiss", group_seed=3000
            )

            # Wrap each group with a weight for the master sampling
            # Group weights: Nordic-Germanic=3.0, Romance-Asian=2.0, Swiss=1.0
            master_group1 = WeightedNodeConfig(group1, 3.0)
            master_group2 = WeightedNodeConfig(group2, 2.0)
            master_group3 = WeightedNodeConfig(group3, 1.0)

            # Master pipeline: 3 groups → AutoSampling → Shuffle → Thread
            master_config = ThreadedConfig(
                ShuffleConfig(
                    AutoSamplingConfig(
                        [master_group1, master_group2, master_group3],
                        seed=12345,
                    ),
                    buffer_size=200,
                    seed=67890,
                ),
                buffer_size=128,
            )

            # Print the tree structure
            print("\n" + "=" * 70)
            print("FULL TREE:")
            print("=" * 70)
            print(named_tree(master_config, "MasterPipeline"))

            print("\nTERSE TREE:")
            print("=" * 70)
            print(terse_tree(master_config, "MasterPipeline"))
            print("=" * 70)

            # Calculate expected proportions
            # WeightedNodeConfig adds weight, so:
            #   master_group1.weight = group1_weight + 3.0 = 11.5 + 3.0 = 14.5
            #   master_group2.weight = group2_weight + 2.0 = 5.5 + 2.0 = 7.5
            #   master_group3.weight = group3_weight + 1.0 = 1.5 + 1.0 = 2.5
            #   total = 24.5
            #
            # For each language, expected proportion =
            #   (master_group_weight / master_total) * (lang_weight / group_total)

            master_weight1 = group1_weight + 3.0  # 14.5
            master_weight2 = group2_weight + 2.0  # 7.5
            master_weight3 = group3_weight + 1.0  # 2.5
            master_total = master_weight1 + master_weight2 + master_weight3  # 24.5

            expected_proportions = {}

            # Group 1
            g1_ratio = master_weight1 / master_total
            for lang, weight, _ in group1_languages:
                expected_proportions[lang] = g1_ratio * (weight / group1_weight)

            # Group 2
            g2_ratio = master_weight2 / master_total
            for lang, weight, _ in group2_languages:
                expected_proportions[lang] = g2_ratio * (weight / group2_weight)

            # Group 3
            g3_ratio = master_weight3 / master_total
            for lang, weight, _ in group3_languages:
                expected_proportions[lang] = g3_ratio * (weight / group3_weight)

            print("\nExpected proportions:")
            for lang, prop in sorted(expected_proportions.items()):
                print(f"  {lang}: {prop:.4f} ({prop * 100:.2f}%)")

            # Read samples and count
            num_samples = 50000
            counts: dict[str, int] = {lang: 0 for lang, _, _ in
                                       group1_languages + group2_languages + group3_languages}

            with master_config as reader:
                for i, record in enumerate(reader):
                    if i >= num_samples:
                        break
                    # Parse language from record (format: "lang:shard:idx")
                    lang = bytes(record).decode().split(":")[0]
                    counts[lang] += 1

            total_read = sum(counts.values())
            assert total_read == num_samples, f"Expected {num_samples}, got {total_read}"

            # Calculate actual proportions
            actual_proportions = {lang: count / total_read for lang, count in counts.items()}

            print(f"\nActual proportions (n={total_read}):")
            for lang in sorted(counts.keys()):
                expected = expected_proportions[lang]
                actual = actual_proportions[lang]
                diff = abs(actual - expected)
                diff_pct = diff / expected * 100 if expected > 0 else 0
                status = "✓" if diff_pct <= 5 else "✗"
                print(f"  {lang}: {actual:.4f} ({counts[lang]:4d}) "
                      f"expected={expected:.4f} diff={diff_pct:.1f}% {status}")

            # Assert all proportions are within 5% of expected
            for lang in counts.keys():
                expected = expected_proportions[lang]
                actual = actual_proportions[lang]
                diff_pct = abs(actual - expected) / expected * 100 if expected > 0 else 0
                assert diff_pct <= 5, (
                    f"Language {lang} proportion {actual:.4f} differs from "
                    f"expected {expected:.4f} by {diff_pct:.1f}% (> 5%)"
                )

            print("\n✓ All language proportions within 5% of expected!")


if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])
