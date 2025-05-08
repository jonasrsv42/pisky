"""
Single-threaded reader and writer for Disky record format.

This module provides the RecordReader and RecordWriter classes for
reading and writing Disky records with a simple, single-threaded API.
"""

from typing import Optional, Iterator, Protocol, Union, Any
from pathlib import Path
from os import PathLike

from ._pisky import (
    PyRecordWriter,
    PyRecordReader,
    PyCorruptionStrategy,
)

# Define a type for path-like objects
PathType = Union[str, Path, PathLike, Any]

# Protocol matching the interface of the Rust Bytes class
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
            
        # Count records in a file
        count = RecordReader.count_records("input.disky")
        print(f"File contains {count} records")
        
        # Count records with a corruption strategy
        count = RecordReader.count_records("input.disky", corruption_strategy=CorruptionStrategy.RECOVER)
        print(f"File contains {count} records")
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
    
    @staticmethod
    def count_records(path: PathType, corruption_strategy=None) -> int:
        """
        Count the number of records in a file without loading the full contents into memory.
        
        This method efficiently iterates through the file, counting records while minimizing
        memory usage by not storing the actual record data.
        
        Args:
            path: Path to the input file. Can be a string, pathlib.Path,
                or any object that can be converted to a string path.
            corruption_strategy: Strategy to handle corrupt records:
                - None or CorruptionStrategy.ERROR: Return an error on corruption (default)
                - CorruptionStrategy.RECOVER: Skip corrupted chunks and continue reading.
                  
        Returns:
            The number of records in the file
            
        Raises:
            IOError: If the file cannot be opened or read
            TypeError: If the path cannot be converted to a string
        """
        path_str = str(path)
        return PyRecordReader.count_records(path_str, corruption_strategy)
    
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