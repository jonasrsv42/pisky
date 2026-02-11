"""Tests for i.i.d. properties of the sampling pipeline.

Correct proportions are necessary but not sufficient for ML training.
We also need samples to be approximately independent (well-mixed).

These tests verify:
1. Run lengths are bounded (no long streaks of same source)
2. Consecutive samples are uncorrelated (transition matrix matches expected)
3. Seeds produce reproducible but different sequences
"""

import tempfile
from collections import Counter
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
)


def create_test_pipeline(base_dir: Path, buffer_size: int = 100, seed: int = 42):
    """Create a test pipeline with 3 languages."""
    languages = [
        ("en", 2.0, 100),  # 50%
        ("de", 1.0, 200),  # 25%
        ("fr", 1.0, 300),  # 25%
    ]

    nodes = []
    for lang, weight, lang_seed in languages:
        lang_dir = base_dir / lang
        lang_dir.mkdir(parents=True, exist_ok=True)

        # Create 3 shards with 200 records each
        for shard_idx in range(3):
            shard_prefix = lang_dir / f"shard_{shard_idx:03d}"
            shards = WriterFileShards.from_prefix(str(shard_prefix))
            with ShardWriterConfig(shards) as writer:
                for i in range(200):
                    writer.write(f"{lang}:{shard_idx}:{i}".encode())

        reader_shards = ReaderFileShards.from_pattern(str(lang_dir), "shard")
        order = RandomRepeat(reader_shards, seed=lang_seed)
        nodes.append(
            NamedNodeConfig(
                lang.upper(),
                WeightedNodeConfig(
                    ShuffleConfig(
                        ShardSequentialConfig(order),
                        buffer_size=buffer_size,
                        seed=lang_seed + 1000,
                    ),
                    weight,
                ),
            )
        )

    return ThreadedConfig(
        ShuffleConfig(
            AutoSamplingConfig(nodes, seed=seed),
            buffer_size=buffer_size * 2,
            seed=seed + 1000,
        ),
        buffer_size=64,
    )


def read_samples(config, n: int) -> list[str]:
    """Read n samples and return list of language codes."""
    samples = []
    with config as reader:
        for i, record in enumerate(reader):
            if i >= n:
                break
            lang = bytes(record).decode().split(":")[0]
            samples.append(lang)
    return samples


def compute_run_lengths(samples: list[str]) -> list[int]:
    """Compute lengths of consecutive same-value runs."""
    if not samples:
        return []

    runs = []
    current_run = 1
    for i in range(1, len(samples)):
        if samples[i] == samples[i - 1]:
            current_run += 1
        else:
            runs.append(current_run)
            current_run = 1
    runs.append(current_run)
    return runs


def compute_transition_matrix(
    samples: list[str], labels: list[str]
) -> dict[str, dict[str, int]]:
    """Compute transition counts: how often does label X follow label Y."""
    matrix = {a: {b: 0 for b in labels} for a in labels}
    for i in range(1, len(samples)):
        prev, curr = samples[i - 1], samples[i]
        matrix[prev][curr] += 1
    return matrix


class TestRunLengthBounds:
    """Test that consecutive same-source runs are bounded."""

    def test_max_run_length_bounded(self):
        """Max run length should be reasonable (not entire sequences of one source)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = create_test_pipeline(Path(tmpdir), buffer_size=100)
            samples = read_samples(config, 5000)

            runs = compute_run_lengths(samples)

            # With proper shuffling, max run should be much smaller than sample count
            # Even with small buffers, max run of 50 would be concerning
            max_run = max(runs)
            avg_run = sum(runs) / len(runs)

            print(f"\nRun statistics (n={len(samples)}, buffer=100):")
            print(f"  Max run: {max_run}")
            print(f"  Avg run: {avg_run:.2f}")
            print(f"  Runs > 10: {sum(1 for r in runs if r > 10)}")

            assert max_run < 30, f"Max run {max_run} is too long, suggests poor mixing"
            assert avg_run < 3, f"Avg run {avg_run:.2f} is too high, suggests poor mixing"

    def test_larger_buffer_shorter_runs(self):
        """Larger shuffle buffers should produce shorter runs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)

            # Small buffer
            config_small = create_test_pipeline(base_dir / "small", buffer_size=50, seed=123)
            samples_small = read_samples(config_small, 3000)
            runs_small = compute_run_lengths(samples_small)

            # Larger buffer
            config_large = create_test_pipeline(base_dir / "large", buffer_size=500, seed=123)
            samples_large = read_samples(config_large, 3000)
            runs_large = compute_run_lengths(samples_large)

            avg_small = sum(runs_small) / len(runs_small)
            avg_large = sum(runs_large) / len(runs_large)

            print(f"\nBuffer size effect:")
            print(f"  Small buffer (50): avg run = {avg_small:.2f}")
            print(f"  Large buffer (500): avg run = {avg_large:.2f}")

            # Larger buffer should give shorter average runs (better mixing)
            assert avg_large <= avg_small, "Larger buffer should improve mixing"


class TestTransitionIndependence:
    """Test that transitions approximate independence (P(Y|X) ≈ P(Y))."""

    def test_transition_probabilities_match_marginal(self):
        """If i.i.d., P(next=Y | current=X) should ≈ P(Y) for all X."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = create_test_pipeline(Path(tmpdir), buffer_size=200)
            samples = read_samples(config, 10000)

            labels = ["en", "de", "fr"]
            marginal = Counter(samples)
            total = sum(marginal.values())
            marginal_probs = {l: marginal[l] / total for l in labels}

            transitions = compute_transition_matrix(samples, labels)

            print(f"\nMarginal probabilities: {marginal_probs}")
            print("\nTransition probabilities P(col | row):")

            max_deviation = 0
            for prev in labels:
                row_total = sum(transitions[prev].values())
                if row_total == 0:
                    continue

                row_probs = {l: transitions[prev][l] / row_total for l in labels}
                print(f"  After {prev}: {row_probs}")

                for curr in labels:
                    deviation = abs(row_probs[curr] - marginal_probs[curr])
                    max_deviation = max(max_deviation, deviation)

            print(f"\nMax deviation from marginal: {max_deviation:.4f}")

            # Transition probs should be within 5% of marginal (allowing for sampling variance)
            assert max_deviation < 0.05, (
                f"Transition probs deviate {max_deviation:.2%} from marginal, "
                "suggests dependence between consecutive samples"
            )


class TestSeedReproducibility:
    """Test that seeds produce reproducible but distinct sequences."""

    def test_same_seed_same_sequence(self):
        """Same seed should produce identical sequence."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)

            config1 = create_test_pipeline(base_dir / "run1", seed=12345)
            samples1 = read_samples(config1, 1000)

            config2 = create_test_pipeline(base_dir / "run2", seed=12345)
            samples2 = read_samples(config2, 1000)

            assert samples1 == samples2, "Same seed should produce identical sequences"

    def test_different_seed_different_sequence(self):
        """Different seeds should produce different sequences."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)

            config1 = create_test_pipeline(base_dir / "run1", seed=11111)
            samples1 = read_samples(config1, 1000)

            config2 = create_test_pipeline(base_dir / "run2", seed=22222)
            samples2 = read_samples(config2, 1000)

            # Sequences should differ (not just in one place)
            differences = sum(1 for a, b in zip(samples1, samples2) if a != b)
            diff_ratio = differences / len(samples1)

            print(f"\nDifferent seeds: {diff_ratio:.1%} of samples differ")

            # With different seeds, should see significant differences
            assert diff_ratio > 0.3, (
                f"Only {diff_ratio:.1%} differ between seeds, "
                "suggests seeds aren't working properly"
            )


class TestNoSourceStarvation:
    """Test that all sources get sampled (no starvation)."""

    def test_all_sources_sampled_in_window(self):
        """All sources should appear within a reasonable window."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = create_test_pipeline(Path(tmpdir), buffer_size=100)
            samples = read_samples(config, 5000)

            labels = {"en", "de", "fr"}
            window_size = 100

            # Check that all sources appear in every window of 100 samples
            windows_missing_source = 0
            for start in range(0, len(samples) - window_size, window_size):
                window = samples[start : start + window_size]
                if not labels.issubset(set(window)):
                    windows_missing_source += 1

            total_windows = (len(samples) - window_size) // window_size
            missing_ratio = windows_missing_source / total_windows

            print(f"\nSource coverage:")
            print(f"  Windows missing a source: {windows_missing_source}/{total_windows}")

            # Allow some windows to miss a source (especially for low-weight sources)
            # but it shouldn't be common
            assert missing_ratio < 0.1, (
                f"{missing_ratio:.1%} of windows missing a source, "
                "suggests source starvation"
            )


if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])
