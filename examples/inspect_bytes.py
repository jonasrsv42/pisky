"""
Script to inspect the PyBytes object returned by Rust.
"""

import os
import tempfile
from pisky import RecordWriter, RecordReader

def inspect_bytes_object():
    """Inspect the bytes object returned by the reader."""
    # Create a temporary file
    with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
        temp_path = temp.name
    
    try:
        # Write a single record
        with RecordWriter(temp_path) as writer:
            writer.write_record(b"Test record for inspection")
        
        # Read the record and inspect it
        with RecordReader(temp_path) as reader:
            record = reader.next_record()
            
            if record is not None:
                print(f"Type: {type(record)}")
                print(f"Dir: {dir(record)}")
                print(f"Methods: ")
                for method in dir(record):
                    if not method.startswith('_'):
                        try:
                            attr = getattr(record, method)
                            if callable(attr):
                                print(f"  - {method}(): {type(attr())}")
                            else:
                                print(f"  - {method}: {type(attr)}")
                        except Exception as e:
                            print(f"  - {method}: ERROR - {e}")
                
                # Try to convert to regular bytes
                if hasattr(record, 'to_bytes'):
                    py_bytes = record.to_bytes()
                    print(f"\nConverted to Python bytes: {py_bytes}")
                    print(f"Decoded: {py_bytes.decode('utf-8')}")
    
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_path):
            os.unlink(temp_path)

if __name__ == "__main__":
    inspect_bytes_object()