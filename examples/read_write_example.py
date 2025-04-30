"""
Example of using the Pisky RecordWriter and RecordReader.

This module demonstrates how to write and read records using Pisky.
"""

import os
import tempfile
from pisky import RecordWriter, RecordReader

def write_and_read_example():
    """
    Example of writing records to a file and then reading them back.
    
    This demonstrates the basic write-then-read workflow.
    """
    # Create a temporary file
    with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
        temp_path = temp.name
    
    try:
        # Write records
        with RecordWriter(temp_path) as writer:
            print(f"Writing records to {temp_path}")
            writer.write_record(b"Hello, world!")
            writer.write_record(b"This is record #2")
            writer.write_record(b"And this is record #3")
        
        # Read records
        print(f"\nReading records from {temp_path}")
        with RecordReader(temp_path) as reader:
            for i, record in enumerate(reader, 1):
                # The record is a pyo3_bytes::PyBytes object
                # Use to_bytes() to convert to regular Python bytes for decode
                print(f"Record {i}: {record.to_bytes().decode('utf-8')}")
    
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_path):
            os.unlink(temp_path)
            print(f"\nDeleted temporary file: {temp_path}")

def write_and_read_manual_example():
    """
    Example of writing and reading records without using context managers.
    
    This demonstrates how to manually manage the writer and reader.
    """
    # Create a temporary file
    with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
        temp_path = temp.name
    
    try:
        # Write records manually
        print(f"Writing records to {temp_path} (manual mode)")
        writer = RecordWriter(temp_path)
        writer.write_record(b"First record")
        writer.write_record(b"Second record")
        writer.flush()  # Explicitly flush data
        writer.close()  # Explicitly close the writer
        
        # Read records manually
        print(f"\nReading records from {temp_path} (manual mode)")
        reader = RecordReader(temp_path)
        count = 0
        
        while True:
            record = reader.next_record()
            if record is None:
                break
            
            count += 1
            # Convert from Rust bytes to Python bytes for decode
            print(f"Record {count}: {record.to_bytes().decode('utf-8')}")
        
        # No need to explicitly close the reader
    
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_path):
            os.unlink(temp_path)
            print(f"\nDeleted temporary file: {temp_path}")

def write_large_file_example(num_records=1000):
    """
    Example of writing and reading a larger number of records.
    
    Args:
        num_records: Number of records to write
    """
    # Create a temporary file
    with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
        temp_path = temp.name
    
    try:
        # Write many records
        print(f"Writing {num_records} records to {temp_path}")
        with RecordWriter(temp_path) as writer:
            for i in range(num_records):
                writer.write_record(f"Record #{i}".encode('utf-8'))
        
        # Get file size
        file_size = os.path.getsize(temp_path)
        print(f"File size: {file_size} bytes")
        
        # Read and count records
        print(f"Reading and counting records from {temp_path}")
        count = 0
        with RecordReader(temp_path) as reader:
            for _ in reader:
                count += 1
        
        print(f"Read {count} records from file")
        print(f"Average record size: {file_size / count:.2f} bytes")
    
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_path):
            os.unlink(temp_path)
            print(f"Deleted temporary file: {temp_path}")

if __name__ == "__main__":
    print("=== Basic Write and Read Example ===")
    write_and_read_example()
    
    print("\n=== Manual Write and Read Example ===")
    write_and_read_manual_example()
    
    print("\n=== Large File Example ===")
    write_large_file_example(1000)