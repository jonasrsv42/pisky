"""
Example demonstrating the corruption recovery feature in Pisky.

This example shows how to use the CorruptionStrategy to handle corrupted files.
"""

import tempfile
from pathlib import Path
from pisky import RecordWriterConfig, RecordReaderConfig, CorruptionStrategy, set_log_level
from pisky.multi_threaded.reader import MultiThreadedConfig as ReaderConfig
from pisky.multi_threaded.writer import MultiThreadedConfig as WriterConfig
from pisky.multi_threaded.writer import FileShards as WriterFileShards
from pisky.shard.file_shards import FileShards as ReaderFileShards
from pisky.shard.order import Sequential


def simple_corruption_recovery_example(temp_dir: Path):
    """
    Example of using the CorruptionStrategy.RECOVER option with RecordReaderConfig.

    Note that corruption recovery operates at the chunk level - if corruption is detected,
    the entire chunk (typically about 1MB of data) will be skipped, not just individual records.
    """
    path = temp_dir / "corrupted.disky"

    # Write many records to ensure multiple chunks (each chunk is ~1MB)
    print(f"Writing records to {path}")
    record_data = b"X" * 1024  # 1KB of data

    with RecordWriterConfig(path) as writer:
        for i in range(2000):
            writer.write(f"Record #{i}: ".encode("utf-8") + record_data)

    file_size = path.stat().st_size
    print(f"Created file of size: {file_size / (1024*1024):.2f} MB")

    # Simulate corruption by inserting invalid data
    with open(path, "r+b") as f:
        f.seek(400)
        f.write(b"CORRUPTION" * 50)

    print(f"Added corruption at position 400")

    # Try to read with default error strategy (will fail)
    print("\nTrying to read with default error strategy (will fail):")
    records_read = 0
    try:
        with RecordReaderConfig(path) as reader:
            for record in reader:
                records_read += 1
                if records_read <= 3:
                    print(f"  Record {records_read}: {bytes(record)[:30]}...")
        print(f"Read {records_read} records successfully")
    except Exception as e:
        print(f"Error reading file: {e}")
        print(f"Read {records_read} records before error")

    # Try to read with recovery strategy
    print("\nTrying to read with recovery strategy:")
    with RecordReaderConfig(path, CorruptionStrategy.RECOVER) as reader:
        record_count = 0
        for record in reader:
            record_count += 1
            if record_count <= 3:
                print(f"  Record {record_count}: {bytes(record)[:30]}...")

        print(f"Successfully read {record_count} records with recovery strategy")
        print(f"Note: Some records may be lost because entire chunks containing corruption are skipped")


def multithreaded_corruption_recovery_example(temp_dir: Path):
    """
    Example of using the CorruptionStrategy.RECOVER option with multi-threaded reader.
    """
    shard_dir = temp_dir / "shards"
    shard_dir.mkdir(exist_ok=True)

    # Write records to multiple shards
    shards = WriterFileShards.from_pattern(str(shard_dir), "shard")
    record_data = b"X" * 1024  # 1KB of data

    with WriterConfig(shards, num_shards=2) as writer:
        print(f"\nWriting records to sharded files (2 shards)")
        for i in range(4000):
            writer.write(f"Record #{i}: ".encode("utf-8") + record_data)
            if i % 1000 == 0 and i > 0:
                print(f"  Wrote {i} records...")

    # List the created shard files
    shard_files = sorted(shard_dir.glob("shard_*"))
    print(f"Created {len(shard_files)} shard files")

    for shard_file in shard_files:
        size_mb = shard_file.stat().st_size / (1024 * 1024)
        print(f"  {shard_file.name}: {size_mb:.2f} MB")

    # Corrupt one of the shards
    if shard_files:
        with open(shard_files[0], "r+b") as f:
            f.seek(400)
            f.write(b"CORRUPTION" * 50)
        print(f"Corrupted shard file at position 400: {shard_files[0].name}")

    # Try to read with default error strategy (will fail)
    print("\nTrying to read with default error strategy (will fail):")
    reader_shards = ReaderFileShards.from_pattern(str(shard_dir), "shard")
    order = Sequential(reader_shards)

    records_read = 0
    try:
        with ReaderConfig(order, num_parallel=2) as reader:
            for record in reader:
                records_read += 1
                if records_read <= 3:
                    print(f"  Record {records_read}: {bytes(record)[:30]}...")
        print(f"Read {records_read} records successfully")
    except Exception as e:
        print(f"Error reading files: {e}")
        print(f"Read {records_read} records before error")

    # Try to read with recovery strategy (corruption_strategy is now on FileShards)
    print("\nTrying to read with recovery strategy:")
    reader_shards = ReaderFileShards.from_pattern(
        str(shard_dir), "shard", corruption_strategy=CorruptionStrategy.RECOVER
    )
    order = Sequential(reader_shards)

    with ReaderConfig(order, num_parallel=2) as reader:
        record_count = 0
        for record in reader:
            record_count += 1
            if record_count <= 3:
                print(f"  Record {record_count}: {bytes(record)[:30]}...")

        print(f"Successfully read {record_count} records with recovery strategy")
        print(f"Note: Some records may be lost because entire chunks containing corruption are skipped")


if __name__ == "__main__":
    # Set logging level to debug for detailed corruption handling information
    print("Setting log level to debug for detailed corruption handling information...")
    set_log_level("debug")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)

        print("=== Simple Corruption Recovery Example ===")
        simple_corruption_recovery_example(temp_dir)

        print("\n=== Multi-threaded Corruption Recovery Example ===")
        multithreaded_corruption_recovery_example(temp_dir)

    print("\nTemporary directory cleaned up automatically")
