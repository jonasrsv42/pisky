"""
Example usage of the Pisky RecordWriter.

This module shows how to use the RecordWriter from Python.
"""

import os
import tempfile
from pisky import RecordWriter

def write_example() -> str:
    """
    Basic example of writing records to a file.
    
    Returns:
        The path to the temporary file with written records
    """
    # Create a temporary file
    with tempfile.NamedTemporaryFile(suffix='.disky', delete=False) as temp:
        temp_path = temp.name
    
    # Create a writer
    with RecordWriter(temp_path) as writer:
        # Write some records
        writer.write_record(b"Record 1")
        writer.write_record(b"Record 2")
        writer.write_record(b"Record 3")
        
        # No need to call close() manually, the context manager does it
    
    return temp_path

def write_example_manual() -> str:
    """
    Example of writing records to a file without using a context manager.
    
    Returns:
        The path to the temporary file with written records
    """
    # Create a temporary file
    with tempfile.NamedTemporaryFile(suffix='.disky', delete=False) as temp:
        temp_path = temp.name
    
    # Create a writer
    writer = RecordWriter(temp_path)
    
    try:
        # Write some records
        writer.write_record(b"Record 1")
        writer.write_record(b"Record 2")
        writer.write_record(b"Record 3")
        
        # Flush ensures all data is written to disk
        writer.flush()
    finally:
        # Always close the writer
        writer.close()
    
    return temp_path

def write_records_from_list(records: list[bytes]) -> str:
    """
    Example of writing a list of records to a file.
    
    Args:
        records: List of byte strings to write as records
        
    Returns:
        The path to the temporary file with written records
    """
    # Create a temporary file
    with tempfile.NamedTemporaryFile(suffix='.disky', delete=False) as temp:
        temp_path = temp.name
    
    with RecordWriter(temp_path) as writer:
        for record in records:
            writer.write_record(record)
    
    return temp_path

if __name__ == "__main__":
    # Example usage
    output_path = write_example()
    file_size = os.path.getsize(output_path)
    print(f"Successfully wrote records to temporary file: {output_path} ({file_size} bytes)")
    
    # Example with a list of records
    records = [
        b"Record A",
        b"Record B",
        b"Record C",
        b"Record D"
    ]
    list_output_path = write_records_from_list(records)
    file_size = os.path.getsize(list_output_path)
    print(f"Successfully wrote records from list to temporary file: {list_output_path} ({file_size} bytes)")
    
    # Note: In a real application, you'd typically want to clean up temporary files
    # when they're no longer needed:
    # os.unlink(output_path)
    # os.unlink(list_output_path)