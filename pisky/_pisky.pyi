from typing import Any, Optional, Protocol, ContextManager

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