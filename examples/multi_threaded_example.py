"""
Example of using the Pisky multi-threaded reader and writer.

This module demonstrates how to write and read records using
the multi-threaded implementations in Pisky.
"""

import tempfile
import time
from pathlib import Path

from pisky.compression import Zstd
from pisky.multi_threaded.reader import MultiThreadedConfig as ReaderConfig
from pisky.multi_threaded.writer import MultiThreadedConfig as WriterConfig
from pisky.multi_threaded.writer import FileShards as WriterFileShards
from pisky.shard.file_shards import FileShards as ReaderFileShards
from pisky.shard.order import Sequential, RandomRepeat


def multi_threaded_write_read_example(temp_dir: Path):
    """
    Example of writing records using multi-threaded writer and then reading them back.
    """
    num_records = 10000

    # Create file shards for writing
    shards = WriterFileShards.from_pattern(str(temp_dir), "shard")

    # Write records with the multi-threaded writer
    start_time = time.time()
    print(f"Writing {num_records} records using multi-threaded writer...")

    with WriterConfig(shards, num_shards=2) as writer:
        for i in range(num_records):
            data = f"Multi-threaded Record #{i}".encode("utf-8")
            writer.write(data)

            if i % 2000 == 0 and i > 0:
                print(f"  Wrote {i} records...")

    write_time = time.time() - start_time
    print(f"Finished writing in {write_time:.2f} seconds")

    # List the created shard files
    shard_files = sorted(temp_dir.glob("shard_*"))
    print(f"Created {len(shard_files)} shard files: {[f.name for f in shard_files]}")

    # Read records with the multi-threaded reader (sequential order)
    reader_shards = ReaderFileShards.from_pattern(str(temp_dir), "shard")
    order = Sequential(reader_shards)

    start_time = time.time()
    print(f"Reading records using multi-threaded reader (sequential order)...")

    with ReaderConfig(order, num_parallel=2) as reader:
        count = 0
        for record in reader:
            count += 1
            if count <= 3:
                print(f"  Sample record: {bytes(record).decode('utf-8')}")

    read_time = time.time() - start_time
    print(f"Finished reading {count} records in {read_time:.2f} seconds")


def random_order_example(temp_dir: Path):
    """
    Example with random repeating order for infinite iteration.
    """
    # First write some data
    shards = WriterFileShards.from_pattern(str(temp_dir / "random"), "shard")
    (temp_dir / "random").mkdir(exist_ok=True)

    with WriterConfig(shards, num_shards=2) as writer:
        for i in range(1000):
            writer.write(f"Record {i}".encode())

    print("\nReading with random repeating order (first 20 records)...")

    reader_shards = ReaderFileShards.from_pattern(str(temp_dir / "random"), "shard")
    order = RandomRepeat(reader_shards, seed=42)  # Seeded for reproducibility

    with ReaderConfig(order, num_parallel=2) as reader:
        for i, record in enumerate(reader):
            if i >= 20:
                break
            print(f"  {bytes(record).decode()}")

    print("(Infinite iteration - stopped after 20 records)")


def custom_settings_example(temp_dir: Path):
    """
    Example with custom settings for the multi-threaded reader and writer.
    """
    custom_dir = temp_dir / "custom"
    custom_dir.mkdir(exist_ok=True)

    # Write records with custom settings
    shards = WriterFileShards.from_pattern(str(custom_dir), "custom")

    with WriterConfig(
        shards,
        num_shards=4,
        worker_threads=2,
        max_bytes_per_writer=100 * 1024 * 1024,  # 100 MB per writer
        task_queue_capacity=500,
        enable_auto_sharding=True,
        compression=Zstd(3),
    ) as writer:
        print("\nWriting records with custom settings (4 shards, zstd compression)...")
        for i in range(1000):
            writer.write(f"Custom Record #{i}".encode("utf-8"))

    # List the created shard files
    shard_files = sorted(custom_dir.glob("custom_*"))
    print(f"Created {len(shard_files)} custom shard files: {[f.name for f in shard_files]}")

    # Read records with custom settings
    reader_shards = ReaderFileShards.from_pattern(str(custom_dir), "custom")
    order = Sequential(reader_shards)

    with ReaderConfig(
        order,
        num_parallel=4,
        worker_threads=2,
        queue_size_mb=100,
    ) as reader:
        print("Reading records with custom settings...")
        count = 0
        for record in reader:
            count += 1
            if count <= 3:
                print(f"  Sample: {bytes(record).decode('utf-8')}")

    print(f"Read {count} records with custom settings")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)

        print("=== Multi-threaded Write and Read Example ===")
        multi_threaded_write_read_example(temp_dir)

        print("\n=== Random Order Example ===")
        random_order_example(temp_dir)

        print("\n=== Custom Settings Example ===")
        custom_settings_example(temp_dir)

    print("\nTemporary directory cleaned up automatically")
