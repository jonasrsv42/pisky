"""
Example usage of the Pisky RecordWriter.

This module shows how to use the RecordWriter from Python.
"""

from pisky import RecordWriter

def write_example(path):
    """
    Basic example of writing records to a file.
    
    Args:
        path: Path to the output file
    """
    # Create a writer
    with RecordWriter(path) as writer:
        # Write some records
        writer.write_record(b"Record 1")
        writer.write_record(b"Record 2")
        writer.write_record(b"Record 3")
        
        # No need to call close() manually, the context manager does it

def write_example_manual(path):
    """
    Example of writing records to a file without using a context manager.
    
    Args:
        path: Path to the output file
    """
    # Create a writer
    writer = RecordWriter(path)
    
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

def write_records_from_list(path, records):
    """
    Example of writing a list of records to a file.
    
    Args:
        path: Path to the output file
        records: List of byte strings to write as records
    """
    with RecordWriter(path) as writer:
        for record in records:
            writer.write_record(record)

if __name__ == "__main__":
    # Example usage
    write_example("example_output.disky")
    print("Successfully wrote records to example_output.disky")
    
    # Example with a list of records
    records = [
        b"Record A",
        b"Record B",
        b"Record C",
        b"Record D"
    ]
    write_records_from_list("list_output.disky", records)
    print("Successfully wrote records from list to list_output.disky")