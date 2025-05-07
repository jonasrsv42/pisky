"""
Pisky: Python bindings for the Disky high-performance record format.

This module provides Python bindings for the Disky library,
which implements the Riegeli record format in Rust.
"""

from typing import Iterator, List, Optional, Protocol, Union, Literal, Any
from pathlib import Path
from os import PathLike
import importlib.metadata

# Define a type for path-like objects (string, Path, or any object with __str__)
PathType = Union[str, Path, PathLike, Any]

try:
    __version__ = importlib.metadata.version("pisky")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"

# Import the low-level bindings
from ._pisky import (
    PyRecordWriter, 
    PyRecordReader, 
    PyMultiThreadedWriter, 
    PyMultiThreadedReader, 
    PyCorruptionStrategy,
    set_log_level as _set_log_level
)

__all__ = ["RecordWriter", "RecordReader", "MultiThreadedWriter", "MultiThreadedReader", 
           "Bytes", "CorruptionStrategy", "set_log_level", "__version__"]

def set_log_level(level: str) -> None:
    """
    Set the logging level for the Disky library.
    
    Args:
        level: One of "trace", "debug", "info", "warn", "error", or "off"
        
    Raises:
        IOError: If an invalid log level is provided
    """
    _set_log_level(level)

# Define a Python enum for CorruptionStrategy
class CorruptionStrategy:
    ERROR = PyCorruptionStrategy.Error
    RECOVER = PyCorruptionStrategy.Recover

# Define a Protocol matching the interface of the Rust Bytes class
class Bytes(Protocol):
    """
    Protocol matching the Rust bytes wrapper class provided by pyo3-bytes.
    
    This protocol represents the interface of bytes from Rust with zero-copy semantics.
    It provides most of the methods that regular Python bytes have,
    plus a to_bytes() method to convert to Python's native bytes.
    """
    def __len__(self) -> int: ...
    def to_bytes(self) -> bytes: ...
    def isalnum(self) -> bool: ...
    def isalpha(self) -> bool: ...
    def isascii(self) -> bool: ...
    def isdigit(self) -> bool: ...
    def islower(self) -> bool: ...
    def isspace(self) -> bool: ...
    def isupper(self) -> bool: ...
    def lower(self) -> 'Bytes': ...
    def upper(self) -> 'Bytes': ...
    def removeprefix(self, prefix: bytes) -> 'Bytes': ...
    def removesuffix(self, suffix: bytes) -> 'Bytes': ...

class RecordWriter:
    """
    A high-performance record writer for the Disky format.
    
    This class provides a Pythonic interface to the underlying
    Rust implementation of the Disky RecordWriter.
    
    Example:
        ```python
        from pisky import RecordWriter
        from pathlib import Path
        
        # Using a string path
        with RecordWriter("output.disky") as writer:
            writer.write_record(b"Record 1")
            writer.write_record(b"Record 2")
        
        # Using a pathlib.Path
        path = Path("/tmp/output.disky")
        with RecordWriter(path) as writer:
            writer.write_record(b"Record 3")
        
        # Manual usage
        writer = RecordWriter("output.disky")
        try:
            writer.write_record(b"Record 3")
            writer.flush()
        finally:
            writer.close()
        ```
    """
    
    def __init__(self, path: PathType):
        """
        Create a new RecordWriter that writes to the specified path.
        
        Args:
            path: Path to the output file. Can be a string, pathlib.Path,
                or any object that can be converted to a string path.
            
        Raises:
            IOError: If the file cannot be created
            TypeError: If the path cannot be converted to a string
        """
        # Convert path to string in Python before passing to Rust
        path_str = str(path)
        self._writer = PyRecordWriter(path_str)
    
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
    
    def __enter__(self) -> "RecordWriter":
        """Enter context manager."""
        return self
    
    def __exit__(self, exc_type: Optional[type], exc_val: Optional[Exception], 
                exc_tb: Optional[object]) -> bool:
        """Exit context manager and close the writer."""
        self.close()
        return False  # Don't suppress exceptions


class RecordReader:
    """
    A high-performance record reader for the Disky format.
    
    This class provides a Pythonic interface to the underlying
    Rust implementation of the Disky RecordReader.
    
    Example:
        ```python
        from pisky import RecordReader, CorruptionStrategy
        from pathlib import Path
        
        # Using a string path
        with RecordReader("input.disky") as reader:
            for record in reader:
                # Note: record is a custom Bytes object, use to_bytes() to get a standard Python bytes
                python_bytes = record.to_bytes()
                print(f"Record: {python_bytes.decode('utf-8')}")
        
        # Using a pathlib.Path
        path = Path("/tmp/input.disky")
        with RecordReader(path, corruption_strategy=CorruptionStrategy.RECOVER) as reader:
            for record in reader:
                print(f"Record: {record.to_bytes().decode('utf-8')}")
        
        # Manual usage
        reader = RecordReader("input.disky")
        try:
            while True:
                record = reader.next_record()
                if record is None:
                    break
                print(f"Record: {record.to_bytes().decode('utf-8')}")
        finally:
            reader.close()
        ```
    """
    
    def __init__(self, path: PathType, corruption_strategy=None):
        """
        Create a new RecordReader that reads from the specified path.
        
        Args:
            path: Path to the input file. Can be a string, pathlib.Path,
                or any object that can be converted to a string path.
            corruption_strategy: Strategy to handle corrupt records:
                - None or CorruptionStrategy.ERROR: Return an error on corruption (default)
                - CorruptionStrategy.RECOVER: Skip corrupted chunks and continue reading.
                  A chunk is a collection of records (typically about 1MB of data),
                  so this setting will drop all records in a corrupted chunk.
            
        Raises:
            IOError: If the file cannot be opened
            TypeError: If the path cannot be converted to a string
        """
        # Convert path to string in Python before passing to Rust
        path_str = str(path)
        self._reader = PyRecordReader(path_str, corruption_strategy)
    
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
        """
        # The actual closing is handled by the Python garbage collector
        # through the Rust Drop trait
        pass
    
    def __iter__(self) -> Iterator[Bytes]:
        """Return an iterator over the records."""
        return RecordIterator(self)
    
    def __enter__(self) -> "RecordReader":
        """Enter context manager."""
        return self
    
    def __exit__(self, exc_type: Optional[type], exc_val: Optional[Exception], 
                exc_tb: Optional[object]) -> bool:
        """Exit context manager and close the reader."""
        self.close()
        return False  # Don't suppress exceptions


class RecordIterator:
    """Iterator for Disky records."""
    
    def __init__(self, reader: RecordReader):
        self._reader = reader
    
    def __iter__(self) -> "RecordIterator":
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
        append: bool = True
    ) -> "MultiThreadedWriter":
        """
        Create a new MultiThreadedWriter that writes to multiple sharded files.
        
        Args:
            dir_path: Directory path for the output files. Can be a string, pathlib.Path,
                or any object that can be converted to a string path.
            prefix: Prefix for shard file names (default: "shard")
            num_shards: Number of shards to create (default: 2)
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
            append
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
    
    def __exit__(self, exc_type: Optional[type], exc_val: Optional[Exception], 
                exc_tb: Optional[object]) -> bool:
        """Exit context manager and close the writer."""
        self.close()
        return False  # Don't suppress exceptions


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
        ```
    """
    
    @staticmethod
    def new_with_shards(
        dir_path: PathType, 
        prefix: str = "shard", 
        num_shards: int = 2, 
        worker_threads: int = 1,
        queue_size_mb: int = 10 * 1024,  # 10 GB in MB
        corruption_strategy = None
    ) -> "MultiThreadedReader":
        """
        Create a new MultiThreadedReader that reads from multiple sharded files.
        
        Args:
            dir_path: Directory path for the input files. Can be a string, pathlib.Path,
                or any object that can be converted to a string path.
            prefix: Prefix for shard file names (default: "shard")
            num_shards: Number of shards to read from (default: 2)
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
            corruption_strategy
        )
        return MultiThreadedReader(reader)
    
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
    
    def __exit__(self, exc_type: Optional[type], exc_val: Optional[Exception], 
                exc_tb: Optional[object]) -> bool:
        """Exit context manager and close the reader."""
        self.close()
        return False  # Don't suppress exceptions