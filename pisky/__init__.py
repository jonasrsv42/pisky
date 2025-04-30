"""
Pisky: Python bindings for the Disky high-performance record format.

This module provides Python bindings for the Disky library,
which implements the Riegeli record format in Rust.
"""

from typing import List, Optional

from ._pisky import PyRecordWriter

__all__ = ["RecordWriter"]

class RecordWriter:
    """
    A high-performance record writer for the Disky format.
    
    This class provides a Pythonic interface to the underlying
    Rust implementation of the Disky RecordWriter.
    
    Example:
        ```python
        from pisky import RecordWriter
        
        # Using a context manager (recommended)
        with RecordWriter("output.disky") as writer:
            writer.write_record(b"Record 1")
            writer.write_record(b"Record 2")
        
        # Manual usage
        writer = RecordWriter("output.disky")
        try:
            writer.write_record(b"Record 3")
            writer.flush()
        finally:
            writer.close()
        ```
    """
    
    def __init__(self, path: str):
        """
        Create a new RecordWriter that writes to the specified path.
        
        Args:
            path: Path to the output file
            
        Raises:
            IOError: If the file cannot be created
        """
        self._writer = PyRecordWriter(path)
    
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