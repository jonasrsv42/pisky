from typing import Any, Iterator, Optional, Protocol, ContextManager

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
    
    def __init__(self, path: str) -> None:
        """
        Create a new PyRecordWriter that writes to the specified path.
        
        Args:
            path: Path to the output file
        
        Raises:
            IOError: If the file cannot be created
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
    
    def __init__(self, path: str) -> None:
        """
        Create a new PyRecordReader that reads from the specified path.
        
        Args:
            path: Path to the input file
        
        Raises:
            IOError: If the file cannot be opened
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