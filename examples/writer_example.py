"""
Example usage of the Pisky RecordWriterConfig.

This module shows how to use the RecordWriterConfig from Python.
"""

import os
import tempfile
from pathlib import Path
from pisky import RecordWriterConfig, RecordReaderConfig, Zstd


def write_example(temp_dir: Path) -> Path:
    """
    Basic example of writing records to a file.

    Returns:
        The path to the file with written records
    """
    path = temp_dir / "basic.disky"

    with RecordWriterConfig(path) as writer:
        writer.write(b"Record 1")
        writer.write(b"Record 2")
        writer.write(b"Record 3")

    return path


def write_large_compression_example(temp_dir: Path, num_records: int = 10000) -> tuple[Path, Path]:
    """
    Example comparing compressed vs uncompressed file sizes with realistic data.

    Returns:
        Tuple of (uncompressed_path, compressed_path)
    """
    uncompressed_path = temp_dir / "large_uncompressed.disky"
    compressed_path = temp_dir / "large_compressed.disky"

    # Generate text-like records that compress well
    sample_texts = [
        b"The quick brown fox jumps over the lazy dog. ",
        b"Lorem ipsum dolor sit amet, consectetur adipiscing elit. ",
        b"Machine learning models require large amounts of training data. ",
        b"Python is a versatile programming language for data science. ",
        b"Compression algorithms reduce storage requirements significantly. ",
    ]

    def generate_record(i: int) -> bytes:
        # Mix of repetitive text (compresses well) and unique identifier
        base = sample_texts[i % len(sample_texts)] * 10
        return f"Record {i:06d}: ".encode() + base

    # Write uncompressed
    with RecordWriterConfig(uncompressed_path) as writer:
        for i in range(num_records):
            writer.write(generate_record(i))

    # Write compressed (zstd level 3 is a good balance of speed/ratio)
    with RecordWriterConfig(compressed_path, compression=Zstd(3)) as writer:
        for i in range(num_records):
            writer.write(generate_record(i))

    return uncompressed_path, compressed_path


def write_records_from_list(temp_dir: Path, records: list[bytes]) -> Path:
    """
    Example of writing a list of records to a file.

    Args:
        records: List of byte strings to write as records

    Returns:
        The path to the file with written records
    """
    path = temp_dir / "from_list.disky"

    with RecordWriterConfig(path) as writer:
        for record in records:
            writer.write(record)

    return path


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)

        # Basic example
        print("=== Basic Write Example ===")
        output_path = write_example(temp_dir)
        file_size = os.path.getsize(output_path)
        print(f"Wrote 3 records: {file_size} bytes")

        # From list
        print("\n=== Write From List Example ===")
        records = [b"Record A", b"Record B", b"Record C", b"Record D"]
        list_path = write_records_from_list(temp_dir, records)
        file_size = os.path.getsize(list_path)
        print(f"Wrote 4 records: {file_size} bytes")

        # Compression comparison with larger data
        print("\n=== Compression Comparison (10k records) ===")
        uncompressed_path, compressed_path = write_large_compression_example(temp_dir)

        uncompressed_size = os.path.getsize(uncompressed_path)
        compressed_size = os.path.getsize(compressed_path)
        ratio = uncompressed_size / compressed_size

        print(f"Uncompressed: {uncompressed_size:,} bytes ({uncompressed_size / 1024 / 1024:.2f} MB)")
        print(f"Compressed:   {compressed_size:,} bytes ({compressed_size / 1024 / 1024:.2f} MB)")
        print(f"Compression ratio: {ratio:.1f}x (saved {100 * (1 - 1/ratio):.1f}%)")

        # Verify we can read back from both files
        print("\n=== Verifying Read-Back ===")
        with RecordReaderConfig(uncompressed_path) as reader:
            uncompressed_records = [bytes(r) for r in reader]

        with RecordReaderConfig(compressed_path) as reader:
            compressed_records = [bytes(r) for r in reader]

        assert len(uncompressed_records) == len(compressed_records) == 10000
        assert uncompressed_records == compressed_records
        print(f"Read {len(compressed_records):,} records from compressed file")
        print(f"First record: {compressed_records[0][:60]}...")
        print(f"Last record:  {compressed_records[-1][:60]}...")
        print("Verified: compressed and uncompressed contents match!")

    print("\nTemporary directory cleaned up automatically")
