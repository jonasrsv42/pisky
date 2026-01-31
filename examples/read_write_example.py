"""
Example of using the Pisky RecordWriterConfig and RecordReaderConfig.

This module demonstrates how to write and read records using Pisky.
"""

import tempfile
from pathlib import Path
from pisky import RecordWriterConfig, RecordReaderConfig


def write_and_read_example(temp_dir: Path):
    """
    Example of writing records to a file and then reading them back.
    """
    path = temp_dir / "data.disky"

    # Write records
    print(f"Writing records to {path}")
    with RecordWriterConfig(path) as writer:
        writer.write(b"Hello, world!")
        writer.write(b"This is record #2")
        writer.write(b"And this is record #3")

    # Read records
    print(f"Reading records from {path}")
    with RecordReaderConfig(path) as reader:
        for i, record in enumerate(reader, 1):
            # Use bytes() to convert to regular Python bytes
            print(f"Record {i}: {bytes(record).decode('utf-8')}")


def write_large_file_example(temp_dir: Path, num_records: int = 1000):
    """
    Example of writing and reading a larger number of records.

    Args:
        num_records: Number of records to write
    """
    path = temp_dir / "large.disky"

    # Write many records
    print(f"\nWriting {num_records} records to {path}")
    with RecordWriterConfig(path) as writer:
        for i in range(num_records):
            writer.write(f"Record #{i}".encode("utf-8"))

    # Get file size
    file_size = path.stat().st_size
    print(f"File size: {file_size} bytes")

    # Read and count records
    print(f"Reading and counting records from {path}")
    count = 0
    with RecordReaderConfig(path) as reader:
        for _ in reader:
            count += 1

    print(f"Read {count} records from file")
    print(f"Average record size: {file_size / count:.2f} bytes")


def count_records_example(temp_dir: Path):
    """
    Example of counting records using the static method.
    """
    path = temp_dir / "count_test.disky"

    # Write some records
    with RecordWriterConfig(path) as writer:
        for i in range(100):
            writer.write(f"Record {i}".encode())

    # Count using static method
    count = RecordReaderConfig.count_records(path)
    print(f"\nCounted {count} records using RecordReaderConfig.count_records()")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)

        print("=== Basic Write and Read Example ===")
        write_and_read_example(temp_dir)

        print("\n=== Large File Example ===")
        write_large_file_example(temp_dir, 1000)

        print("\n=== Count Records Example ===")
        count_records_example(temp_dir)

    print("\nTemporary directory cleaned up automatically")
