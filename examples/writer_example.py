"""
Example usage of the Pisky RecordWriterConfig.

This module shows how to use the RecordWriterConfig from Python.
"""

import os
import tempfile
from pathlib import Path
from pisky import RecordWriterConfig, Zstd


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


def write_with_compression_example(temp_dir: Path) -> Path:
    """
    Example of writing records with zstd compression.

    Returns:
        The path to the file with written records
    """
    path = temp_dir / "compressed.disky"

    with RecordWriterConfig(path, compression=Zstd(5)) as writer:
        writer.write(b"Record 1")
        writer.write(b"Record 2")
        writer.write(b"Record 3")

    return path


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
        output_path = write_example(temp_dir)
        file_size = os.path.getsize(output_path)
        print(f"Wrote records to: {output_path} ({file_size} bytes)")

        # With compression
        compressed_path = write_with_compression_example(temp_dir)
        compressed_size = os.path.getsize(compressed_path)
        print(f"Wrote compressed records: {compressed_path} ({compressed_size} bytes)")

        # From list
        records = [b"Record A", b"Record B", b"Record C", b"Record D"]
        list_path = write_records_from_list(temp_dir, records)
        file_size = os.path.getsize(list_path)
        print(f"Wrote records from list: {list_path} ({file_size} bytes)")

    print("Temporary directory cleaned up automatically")
