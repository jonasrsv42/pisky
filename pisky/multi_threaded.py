"""
Multi-threaded reader and writer for Disky record format.

This module provides MultiThreadedReader and MultiThreadedWriter classes for
parallel reading and writing of Disky records across multiple files.
"""

from os import PathLike
from pathlib import Path
from typing import Any, Iterator, Optional, Sequence, Union

from ._pisky import PyMultiThreadedReader, PyMultiThreadedWriter
from .single_threaded import Bytes

# Define a type for path-like objects
PathType = Union[str, Path, PathLike, Any]


class MultiThreadedReaderIterator:
    """Iterator for multi-threaded Disky records."""

    def __init__(self, reader: "MultiThreadedReader"):
        self._reader = reader

    def __iter__(self) -> "MultiThreadedReaderIterator":
        return self

    def __next__(self) -> Bytes:
        record = self._reader.next_record()
        if record is None:
            raise StopIteration
        return record


class MultiThreadedWriter:
    """
    A high-performance multi-threaded record writer for Disky format.

    This class provides a Pythonic interface to the underlying Rust
    implementation of the Disky MultiThreadedWriter. It allows for
    parallel, multi-threaded record writing with optional sharding across
    multiple files.

    Example:
        ```python
        from pisky import MultiThreadedWriter
        from pathlib import Path

        # Using a string path
        with MultiThreadedWriter.new_with_shards(
            dir_path="/tmp/output",
            prefix="shard",
            num_shards=3,
            worker_threads=4,
            enable_auto_sharding=True
        ) as writer:
            for i in range(1000):
                writer.write_record(f"Record #{i}".encode('utf-8'))

        # Using a pathlib.Path
        output_dir = Path("/tmp/pathlib_output")
        output_dir.mkdir(exist_ok=True)
        with MultiThreadedWriter.new_with_shards(
            dir_path=output_dir,
            prefix="shard",
            num_shards=2
        ) as writer:
            for i in range(100):
                writer.write_record(f"Path Record #{i}".encode('utf-8'))
        ```
    """

    @staticmethod
    def new_with_shards(
        dir_path: PathType,
        prefix: str = "shard",
        num_shards: int = 2,
        worker_threads: Optional[int] = None,
        max_bytes_per_writer: Optional[int] = 10 * 1024 * 1024 * 1024,  # 10 GB
        task_queue_capacity: int = 2000,
        enable_auto_sharding: bool = True,
        append: bool = True,
    ) -> "MultiThreadedWriter":
        """
        Create a new MultiThreadedWriter that writes to multiple sharded files.

        Args:
            dir_path: Directory path for the output files. Can be a string, pathlib.Path,
                or any object that can be converted to a string path.
            prefix: Prefix for shard file names (default: "shard")
            num_shards: Number of shards to create and manage in the active resource pool (default: 2).
                This controls how many file handles are maintained concurrently.
            worker_threads: Number of worker threads to use (default: auto)
            max_bytes_per_writer: Maximum bytes per writer (default: 10 GB)
            task_queue_capacity: Capacity of the task queue (default: 2000)
            enable_auto_sharding: Whether to enable auto-sharding (default: True)
            append: Whether to append to existing shards (default: True)

        Returns:
            A new MultiThreadedWriter instance

        Raises:
            IOError: If the writer cannot be created
            TypeError: If the path cannot be converted to a string
        """
        # Convert dir_path to string in Python before passing to Rust
        dir_path_str = str(dir_path)
        writer = PyMultiThreadedWriter.new_with_shards(
            dir_path_str,
            prefix,
            num_shards,
            worker_threads,
            max_bytes_per_writer,
            task_queue_capacity,
            enable_auto_sharding,
            append,
        )
        return MultiThreadedWriter(writer)

    def __init__(self, writer: PyMultiThreadedWriter):
        """
        Initialize with a PyMultiThreadedWriter instance.

        Args:
            writer: The low-level writer instance
        """
        self._writer = writer

    def write_record(self, data: bytes) -> None:
        """
        Write a record to the file.

        Args:
            data: The record data to write

        Raises:
            IOError: If the write fails or if the writer is closed
        """
        self._writer.write_record(data)

    def flush(self) -> None:
        """
        Flush any buffered records to disk.

        Raises:
            IOError: If the flush fails or if the writer is closed
        """
        self._writer.flush()

    def close(self) -> None:
        """
        Close the writer, flushing any remaining data.

        After closing, no more records can be written.

        Raises:
            IOError: If the close operation fails
        """
        self._writer.close()

    def pending_tasks(self) -> int:
        """
        Get the number of pending tasks in the queue.

        Returns:
            The number of pending tasks

        Raises:
            IOError: If there is an error accessing the writer
        """
        return self._writer.pending_tasks()

    def available_writers(self) -> int:
        """
        Get the number of available writer resources.

        Returns:
            The number of available writers

        Raises:
            IOError: If there is an error accessing the writer
        """
        return self._writer.available_writers()

    def __enter__(self) -> "MultiThreadedWriter":
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[Exception],
        exc_tb: Optional[object],
    ) -> bool:
        """Exit context manager and close the writer."""
        self.close()
        return False  # Don't suppress exceptions


class MultiThreadedReader:
    """
    A high-performance multi-threaded record reader for Disky format.

    This class provides a Pythonic interface to the underlying Rust
    implementation of the Disky MultiThreadedReader. It allows for
    parallel, multi-threaded record reading from multiple sharded files.

    Example:
        ```python
        from pisky import MultiThreadedReader, CorruptionStrategy
        from pathlib import Path

        # Using a string path with default settings
        with MultiThreadedReader.new_with_shards(
            dir_path="/tmp/output"
        ) as reader:
            for record in reader:
                # Note: record is a custom Bytes object, use to_bytes() to get a standard Python bytes
                python_bytes = record.to_bytes()
                print(f"Record: {python_bytes.decode('utf-8')}")

        # Using a pathlib.Path with custom settings including corruption recovery
        input_dir = Path("/tmp/custom")
        with MultiThreadedReader.new_with_shards(
            dir_path=input_dir,
            prefix="custom_shard",
            num_shards=4,
            worker_threads=4,
            queue_size_mb=1024,  # 1 GB
            corruption_strategy=CorruptionStrategy.RECOVER
        ) as reader:
            for record in reader:
                print(f"Record: {record.to_bytes().decode('utf-8')}")

        # Using explicit shard paths
        shard_paths = ["/tmp/shards/shard_001.disky", "/tmp/shards/shard_002.disky"]
        with MultiThreadedReader.new_with_shard_paths(
            shard_paths=shard_paths,
            num_shards=2,
            worker_threads=2
        ) as reader:
            for record in reader:
                print(f"Record: {record.to_bytes().decode('utf-8')}")

        # Using random shard paths (reads indefinitely in random order)
        training_shards = [f"/tmp/training/shard_{i:03d}.disky" for i in range(100)]
        with MultiThreadedReader.new_with_random_shard_paths(
            shard_paths=training_shards,
            num_shards=4,
            worker_threads=4
        ) as reader:
            # This will read indefinitely, reshuffling each time all shards are processed
            for i, record in enumerate(reader):
                if i >= 1000000:  # process 1M examples then stop
                    break
                process_training_example(record)

        # Count records in sharded files
        count = MultiThreadedReader.count_records_with_shards(
            dir_path="/tmp/output",
            prefix="shard",
            num_shards=2
        )
        print(f"Total records: {count}")

        # Count records in specific shard paths
        count = MultiThreadedReader.count_records_with_shard_paths(
            shard_paths=["/tmp/shards/shard_001.disky", "/tmp/shards/shard_002.disky"],
            num_shards=2
        )
        print(f"Total records: {count}")
        ```
    """

    @staticmethod
    def new_with_shards(
        dir_path: PathType,
        prefix: str = "shard",
        num_shards: int = 2,
        worker_threads: int = 1,
        queue_size_mb: int = 10 * 1024,  # 10 GB in MB
        corruption_strategy=None,
    ) -> "MultiThreadedReader":
        """
        Create a new MultiThreadedReader that reads from multiple sharded files.

        Args:
            dir_path: Directory path for the input files. Can be a string, pathlib.Path,
                or any object that can be converted to a string path.
            prefix: Prefix for shard file names (default: "shard")
            num_shards: Number of shards to read from concurrently (default: 2).
                This controls how many file handles are kept open in the active resource pool,
                which impacts I/O parallelism and memory usage.
            worker_threads: Number of worker threads to use (default: 1)
            queue_size_mb: Size of the queue in megabytes (default: 10 GB)
            corruption_strategy: Strategy to handle corrupt records:
                - None or CorruptionStrategy.ERROR: Return an error on corruption (default)
                - CorruptionStrategy.RECOVER: Skip corrupted chunks and continue reading.
                  A chunk is a collection of records (typically about 1MB of data),
                  so this setting will drop all records in a corrupted chunk.

        Returns:
            A new MultiThreadedReader instance

        Raises:
            IOError: If the reader cannot be created
            TypeError: If the path cannot be converted to a string
        """
        # Convert dir_path to string in Python before passing to Rust
        dir_path_str = str(dir_path)
        reader = PyMultiThreadedReader.new_with_shards(
            dir_path_str,
            prefix,
            num_shards,
            worker_threads,
            queue_size_mb,
            corruption_strategy,
        )
        return MultiThreadedReader(reader)

    @staticmethod
    def new_with_shard_paths(
        shard_paths: Sequence[PathType],
        num_shards: int = 2,
        worker_threads: int = 1,
        queue_size_mb: int = 10 * 1024,  # 10 GB in MB
        corruption_strategy=None,
    ) -> "MultiThreadedReader":
        """
        Create a new MultiThreadedReader that reads from specific shard paths.

        This method allows you to directly specify the paths to each shard file
        rather than using a directory and a glob pattern.

        Args:
            shard_paths: Sequence of paths to shard files. Each path can be a string, pathlib.Path,
                or any object that can be converted to a string path.
            num_shards: Number of shards to read in parallel (default: 2).
                This controls how many file handles are kept open in the active resource pool,
                which impacts I/O parallelism and memory usage. Note that this can be different
                from the total number of shard files in shard_paths.
            worker_threads: Number of worker threads to use (default: 1)
            queue_size_mb: Size of the queue in megabytes (default: 10 GB)
            corruption_strategy: Strategy to handle corrupt records:
                - None or CorruptionStrategy.ERROR: Return an error on corruption (default)
                - CorruptionStrategy.RECOVER: Skip corrupted chunks and continue reading.
                  A chunk is a collection of records (typically about 1MB of data),
                  so this setting will drop all records in a corrupted chunk.

        Returns:
            A new MultiThreadedReader instance

        Raises:
            IOError: If the reader cannot be created or if the shards cannot be opened
            TypeError: If a path cannot be converted to a string
        """
        # Convert each path to string
        path_strs = [str(path) for path in shard_paths]
        reader = PyMultiThreadedReader.new_with_shard_paths(
            path_strs, num_shards, worker_threads, queue_size_mb, corruption_strategy
        )
        return MultiThreadedReader(reader)

    @staticmethod
    def new_with_random_shard_paths(
        shard_paths: Sequence[PathType],
        num_shards: int = 2,
        worker_threads: int = 1,
        queue_size_mb: int = 10 * 1024,  # 10 GB in MB
        corruption_strategy=None,
    ) -> "MultiThreadedReader":
        """
        Create a new MultiThreadedReader that reads from shard paths in random order indefinitely.

        This method creates a reader that:
        1. Reads shards in a randomized order
        2. Repeats reading shards indefinitely (will never reach EOF)
        3. Reshuffles the order each time all shards have been processed

        This is particularly useful for ML training where random sampling and repeated
        passes over the data are desired.

        Args:
            shard_paths: Sequence of paths to shard files. Each path can be a string, pathlib.Path,
                or any object that can be converted to a string path.
            num_shards: Number of shards to read in parallel (default: 2).
                This controls how many file handles are kept open in the active resource pool,
                which impacts I/O parallelism and memory usage. Note that this can be different
                from the total number of shard files in shard_paths.
            worker_threads: Number of worker threads to use (default: 1)
            queue_size_mb: Size of the queue in megabytes (default: 10 GB)
            corruption_strategy: Strategy to handle corrupt records:
                - None or CorruptionStrategy.ERROR: Return an error on corruption (default)
                - CorruptionStrategy.RECOVER: Skip corrupted chunks and continue reading.
                  A chunk is a collection of records (typically about 1MB of data),
                  so this setting will drop all records in a corrupted chunk.

        Returns:
            A new MultiThreadedReader instance that reads shards randomly and indefinitely

        Raises:
            IOError: If the reader cannot be created or if the shards cannot be opened
            TypeError: If a path cannot be converted to a string
        """
        # Convert each path to string
        path_strs = [str(path) for path in shard_paths]
        reader = PyMultiThreadedReader.new_with_random_shard_paths(
            path_strs, num_shards, worker_threads, queue_size_mb, corruption_strategy
        )
        return MultiThreadedReader(reader)

    @staticmethod
    def count_records_with_shards(
        dir_path: PathType,
        prefix: str = "shard",
        num_shards: int = 2,
        worker_threads: int = 1,
        queue_size_mb: int = 10 * 1024,  # 10 GB in MB
        corruption_strategy=None,
    ) -> int:
        """
        Count the total number of records in multiple sharded files without loading them all into memory.

        This method efficiently counts records across all shards in a directory with a given prefix.

        Args:
            dir_path: Directory path for the input files. Can be a string, pathlib.Path,
                or any object that can be converted to a string path.
            prefix: Prefix for shard file names (default: "shard")
            num_shards: Number of shards to read in parallel (default: 2).
                This controls how many file handles are kept open in the active resource pool,
                which impacts I/O parallelism and memory usage.
            worker_threads: Number of worker threads to use (default: 1)
            queue_size_mb: Size of the queue in megabytes (default: 10 GB)
            corruption_strategy: Strategy to handle corrupt records:
                - None or CorruptionStrategy.ERROR: Return an error on corruption (default)
                - CorruptionStrategy.RECOVER: Skip corrupted chunks and continue reading.
                  A chunk is a collection of records (typically about 1MB of data),
                  so this setting will drop all records in a corrupted chunk.

        Returns:
            The total number of records across all shards

        Raises:
            IOError: If the shards cannot be opened or read
            TypeError: If the path cannot be converted to a string
        """
        # Convert dir_path to string in Python before passing to Rust
        dir_path_str = str(dir_path)
        return PyMultiThreadedReader.count_records_with_shards(
            dir_path_str,
            prefix,
            num_shards,
            worker_threads,
            queue_size_mb,
            corruption_strategy,
        )

    @staticmethod
    def count_records_with_shard_paths(
        shard_paths: Sequence[PathType],
        num_shards: int = 2,
        worker_threads: int = 1,
        queue_size_mb: int = 10 * 1024,  # 10 GB in MB
        corruption_strategy=None,
    ) -> int:
        """
        Count the total number of records in specific shard paths without loading them all into memory.

        This method efficiently counts records across all specified shard files.

        Args:
            shard_paths: Sequence of paths to shard files. Each path can be a string, pathlib.Path,
                or any object that can be converted to a string path.
            num_shards: Number of shards to read in parallel (default: 2).
                This controls how many file handles are kept open in the active resource pool,
                which impacts I/O parallelism and memory usage. Note that this can be different
                from the total number of shard files in shard_paths.
            worker_threads: Number of worker threads to use (default: 1)
            queue_size_mb: Size of the queue in megabytes (default: 10 GB)
            corruption_strategy: Strategy to handle corrupt records:
                - None or CorruptionStrategy.ERROR: Return an error on corruption (default)
                - CorruptionStrategy.RECOVER: Skip corrupted chunks and continue reading.
                  A chunk is a collection of records (typically about 1MB of data),
                  so this setting will drop all records in a corrupted chunk.

        Returns:
            The total number of records across all shard paths

        Raises:
            IOError: If the shards cannot be opened or read
            TypeError: If a path cannot be converted to a string
        """
        # Convert each path to string
        path_strs = [str(path) for path in shard_paths]
        return PyMultiThreadedReader.count_records_with_shard_paths(
            path_strs, num_shards, worker_threads, queue_size_mb, corruption_strategy
        )

    def __init__(self, reader: PyMultiThreadedReader):
        """
        Initialize with a PyMultiThreadedReader instance.

        Args:
            reader: The low-level reader instance
        """
        self._reader = reader

    def next_record(self) -> Optional[Bytes]:
        """
        Read the next record from the file.

        Returns:
            The record data as a Bytes-like object, or None if end of file is reached.
            You can use to_bytes() method to convert to a regular Python bytes object.

        Raises:
            IOError: If the read fails
        """
        return self._reader.next_record()

    def close(self) -> None:
        """
        Close the reader.

        After closing, no more records can be read.

        Raises:
            IOError: If the close operation fails
        """
        self._reader.close()

    def queued_records(self) -> int:
        """
        Get the number of records currently in the queue.

        Returns:
            The number of queued records

        Raises:
            IOError: If there is an error accessing the reader
        """
        return self._reader.queued_records()

    def queued_bytes(self) -> int:
        """
        Get the total size of queued records in bytes.

        Returns:
            The number of bytes in the queue

        Raises:
            IOError: If there is an error accessing the reader
        """
        return self._reader.queued_bytes()

    def __iter__(self) -> MultiThreadedReaderIterator:
        """Return an iterator over the records."""
        return MultiThreadedReaderIterator(self)

    def __enter__(self) -> "MultiThreadedReader":
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[Exception],
        exc_tb: Optional[object],
    ) -> bool:
        """Exit context manager and close the reader."""
        self.close()
        return False  # Don't suppress exceptions
