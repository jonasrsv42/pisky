from typing import Any, Iterator, Optional, Protocol, ContextManager, Literal, Union, overload
from pathlib import Path
from os import PathLike

# Define a type for path-like objects (string, Path, or any object with __str__)
PathType = Union[str, Path, PathLike, Any]

def set_log_level(level_str: str) -> None:
    """
    Set the logging level for the Disky library.
    
    Args:
        level_str: One of "trace", "debug", "info", "warn", "error", or "off"
        
    Raises:
        IOError: If an invalid log level is provided
    """
    ...

class PyCorruptionStrategy:
    """
    Enum for corruption handling strategies in Disky.
    """
    Error: 'PyCorruptionStrategy'
    Recover: 'PyCorruptionStrategy'

class Bytes:
    """
    Rust bytes wrapper class provided by pyo3-bytes.
    
    This class represents bytes from Rust with zero-copy semantics.
    It provides most of the methods that regular Python bytes have,
    plus a to_bytes() method to convert to Python's native bytes.
    """
    def __len__(self) -> int: ...
    def __contains__(self, item: bytes) -> bool: ...
    def __getitem__(self, key: int | slice) -> 'Bytes': ...
    def __add__(self, other: bytes | 'Bytes') -> 'Bytes': ...
    def __radd__(self, other: bytes | 'Bytes') -> 'Bytes': ...
    def __mul__(self, other: int) -> 'Bytes': ...
    def __rmul__(self, other: int) -> 'Bytes': ...
    def __eq__(self, other: object) -> bool: ...
    def __ne__(self, other: object) -> bool: ...
    def __lt__(self, other: bytes | 'Bytes') -> bool: ...
    def __le__(self, other: bytes | 'Bytes') -> bool: ...
    def __gt__(self, other: bytes | 'Bytes') -> bool: ...
    def __ge__(self, other: bytes | 'Bytes') -> bool: ...
    
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
    
    def to_bytes(self) -> bytes:
        """Convert to a standard Python bytes object."""
        ...

class PyRecordWriter(ContextManager["PyRecordWriter"]):
    """
    Low-level Python bindings for Disky's record writer.
    
    This class provides a direct interface to the Rust implementation.
    For a more Pythonic API, use the `RecordWriter` class.
    """
    
    def __init__(self, path: PathType) -> None:
        """
        Create a new PyRecordWriter that writes to the specified path.
        
        Args:
            path: Path to the output file. Can be a string, pathlib.Path,
                or any object that can be converted to a string path.
        
        Raises:
            IOError: If the file cannot be created
            TypeError: If the path cannot be converted to a string
        """
        ...
    
    def write_record(self, data: bytes) -> None:
        """
        Write a record to the file.
        
        Args:
            data: The record data to write
            
        Raises:
            IOError: If the write fails or if the writer is closed
        """
        ...
    
    def flush(self) -> None:
        """
        Flush any buffered records to disk.
        
        Raises:
            IOError: If the flush fails or if the writer is closed
        """
        ...
    
    def close(self) -> None:
        """
        Close the writer, flushing any remaining data.
        
        After closing, no more records can be written.
        
        Raises:
            IOError: If the close operation fails
        """
        ...
    
    def __enter__(self) -> "PyRecordWriter":
        """Enter context manager."""
        ...
    
    def __exit__(self, exc_type: Optional[type], exc_val: Optional[Exception], 
                exc_tb: Optional[Any]) -> bool:
        """Exit context manager and close the writer."""
        ...

class PyRecordReader(Iterator[Bytes]):
    """
    Low-level Python bindings for Disky's record reader.
    
    This class provides a direct interface to the Rust implementation.
    For a more Pythonic API, use the `RecordReader` class.
    """
    
    def __init__(self, path: PathType, corruption_strategy: Optional[PyCorruptionStrategy] = None) -> None:
        """
        Create a new PyRecordReader that reads from the specified path.
        
        Args:
            path: Path to the input file. Can be a string, pathlib.Path,
                or any object that can be converted to a string path.
            corruption_strategy: Strategy to handle corrupted records
        
        Raises:
            IOError: If the file cannot be opened
            TypeError: If the path cannot be converted to a string
        """
        ...
    
    @staticmethod
    def count_records(path: PathType, corruption_strategy: Optional[PyCorruptionStrategy] = None) -> int:
        """
        Count the number of records in a file without loading the full contents into memory.
        
        Args:
            path: Path to the input file. Can be a string, pathlib.Path,
                or any object that can be converted to a string path.
            corruption_strategy: Strategy to handle corrupted records
            
        Returns:
            The number of records in the file
            
        Raises:
            IOError: If the file cannot be opened or read
            TypeError: If the path cannot be converted to a string
        """
        ...
    
    def next_record(self) -> Optional[Bytes]:
        """
        Read the next record from the file.
        
        Returns:
            The record data as a Bytes object, or None if end of file is reached
            
        Raises:
            IOError: If the read fails
        """
        ...
    
    def __iter__(self) -> "PyRecordReader":
        """Return an iterator over the records."""
        ...
    
    def __next__(self) -> Optional[Bytes]:
        """Get the next record or None if at EOF."""
        ...
        
class PyMultiThreadedWriter(ContextManager["PyMultiThreadedWriter"]):
    """
    Low-level Python bindings for Disky's multi-threaded writer.
    
    This class provides a direct interface to the Rust implementation.
    For a more Pythonic API, use the `MultiThreadedWriter` class.
    """
    
    @staticmethod
    def new_with_shards(
        dir_path: PathType,
        prefix: str = "shard",
        num_shards: int = 2,
        worker_threads: Optional[int] = None,
        max_bytes_per_writer: Optional[int] = None,
        task_queue_capacity: Optional[int] = None,
        enable_auto_sharding: Optional[bool] = None,
        append: Optional[bool] = None
    ) -> "PyMultiThreadedWriter": ...
    
    def write_record(self, data: bytes) -> None: ...
    def flush(self) -> None: ...
    def close(self) -> None: ...
    def pending_tasks(self) -> int: ...
    def available_writers(self) -> int: ...
    def __enter__(self) -> "PyMultiThreadedWriter": ...
    def __exit__(
        self, 
        exc_type: Optional[type], 
        exc_val: Optional[Exception], 
        exc_tb: Optional[Any]
    ) -> bool: ...

class PyMultiThreadedReader(Iterator[Bytes], ContextManager["PyMultiThreadedReader"]):
    """
    Low-level Python bindings for Disky's multi-threaded reader.
    
    This class provides a direct interface to the Rust implementation.
    For a more Pythonic API, use the `MultiThreadedReader` class.
    """
    
    @staticmethod
    def new_with_shards(
        dir_path: PathType,
        prefix: str = "shard",
        num_shards: int = 2,
        worker_threads: Optional[int] = None,
        queue_size_mb: Optional[int] = None,
        corruption_strategy: Optional[PyCorruptionStrategy] = None
    ) -> "PyMultiThreadedReader": 
        """
        Create a new PyMultiThreadedReader that reads from sharded files in a directory.
        
        Args:
            dir_path: Directory containing the sharded files
            prefix: File prefix for the shards
            num_shards: Number of shards to read in parallel. Controls how many file handles
                are kept in the active resource pool, which impacts I/O parallelism.
            worker_threads: Number of worker threads (default: 1)
            queue_size_mb: Size of the internal record queue in MB (default: 8)
            corruption_strategy: Strategy to handle corrupted records
            
        Returns:
            A new PyMultiThreadedReader instance
            
        Raises:
            IOError: If the shards cannot be located or opened
        """
        ...
    
    @staticmethod
    def count_records_with_shards(
        dir_path: PathType,
        prefix: str = "shard",
        num_shards: int = 2,
        worker_threads: Optional[int] = None,
        queue_size_mb: Optional[int] = None,
        corruption_strategy: Optional[PyCorruptionStrategy] = None
    ) -> int:
        """
        Count the number of records in sharded files in a directory without loading them into memory.
        
        Args:
            dir_path: Directory containing the sharded files
            prefix: File prefix for the shards
            num_shards: Number of shards to read in parallel. Controls how many file handles
                are kept in the active resource pool, which impacts I/O parallelism.
            worker_threads: Number of worker threads (default: 1)
            queue_size_mb: Size of the internal record queue in MB (default: 8)
            corruption_strategy: Strategy to handle corrupted records
            
        Returns:
            The total number of records across all shards
            
        Raises:
            IOError: If the shards cannot be located or opened
        """
        ...
    
    @staticmethod
    def new_with_shard_paths(
        shard_paths: list[PathType],
        num_shards: int = 2,
        worker_threads: Optional[int] = None,
        queue_size_mb: Optional[int] = None,
        corruption_strategy: Optional[PyCorruptionStrategy] = None
    ) -> "PyMultiThreadedReader":
        """
        Create a new PyMultiThreadedReader that reads from specific shard paths.
        
        This method allows directly specifying the paths to each shard file rather
        than using a directory and a glob pattern.
        
        Args:
            shard_paths: List of paths to shard files
            num_shards: Number of shards to read in parallel. Controls how many file handles
                are kept in the active resource pool, which impacts I/O parallelism. Can be
                different from the total number of shard files in shard_paths.
            worker_threads: Number of worker threads (default: 1)
            queue_size_mb: Size of the internal record queue in MB (default: 8)
            corruption_strategy: Strategy to handle corrupted records
            
        Returns:
            A new PyMultiThreadedReader instance
            
        Raises:
            IOError: If the shards cannot be located or opened
        """
        ...
    
    @staticmethod
    def count_records_with_shard_paths(
        shard_paths: list[PathType],
        num_shards: int = 2,
        worker_threads: Optional[int] = None,
        queue_size_mb: Optional[int] = None,
        corruption_strategy: Optional[PyCorruptionStrategy] = None
    ) -> int:
        """
        Count the number of records in specific shard paths without loading them into memory.
        
        Args:
            shard_paths: List of paths to shard files
            num_shards: Number of shards to read in parallel. Controls how many file handles
                are kept in the active resource pool, which impacts I/O parallelism. Can be
                different from the total number of shard files in shard_paths.
            worker_threads: Number of worker threads (default: 1)
            queue_size_mb: Size of the internal record queue in MB (default: 8)
            corruption_strategy: Strategy to handle corrupted records
            
        Returns:
            The total number of records across all shards
            
        Raises:
            IOError: If the shards cannot be located or opened
        """
        ...
    
    @staticmethod
    def new_with_random_shard_paths(
        shard_paths: list[PathType],
        num_shards: int = 2,
        worker_threads: Optional[int] = None,
        queue_size_mb: Optional[int] = None,
        corruption_strategy: Optional[PyCorruptionStrategy] = None
    ) -> "PyMultiThreadedReader":
        """
        Create a new PyMultiThreadedReader that reads from shard paths in random order indefinitely.
        
        This method creates a reader that:
        1. Reads shards in a randomized order
        2. Repeats reading shards indefinitely (will never reach EOF)
        3. Reshuffles the order each time all shards have been processed
        
        This is particularly useful for ML training where random sampling and repeated 
        passes over the data are desired.
        
        Args:
            shard_paths: List of paths to shard files
            num_shards: Number of shards to read in parallel. Controls how many file handles
                are kept in the active resource pool, which impacts I/O parallelism. Can be
                different from the total number of shard files in shard_paths.
            worker_threads: Number of worker threads (default: 1)
            queue_size_mb: Size of the internal record queue in MB (default: 8)
            corruption_strategy: Strategy to handle corrupted records
            
        Returns:
            A new PyMultiThreadedReader instance that reads shards randomly and indefinitely
            
        Raises:
            IOError: If the shards cannot be located or opened
        """
        ...
    
    def next_record(self) -> Optional[Bytes]: ...
    def close(self) -> None: ...
    def queued_records(self) -> int: ...
    def queued_bytes(self) -> int: ...
    def __iter__(self) -> "PyMultiThreadedReader": ...
    def __next__(self) -> Optional[Bytes]: ...
    def __enter__(self) -> "PyMultiThreadedReader": ...
    def __exit__(
        self, 
        exc_type: Optional[type], 
        exc_val: Optional[Exception], 
        exc_tb: Optional[Any]
    ) -> bool: ...
