#!/usr/bin/env python3
"""
Example consumer application for pisky.

This script demonstrates how to use pisky in a project that depends on it.
"""
import tempfile
import os
from pisky import RecordWriter, RecordReader, MultiThreadedWriter, MultiThreadedReader

def simple_example():
    """Demonstrate basic single-threaded read/write functionality."""
    print("Running simple example...")
    
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        temp_path = tmp.name
    
    try:
        # Write records
        with RecordWriter(temp_path) as writer:
            for i in range(10):
                writer.write_record(f"Record {i}".encode())
            print(f"Wrote 10 records to {temp_path}")
        
        # Read records
        with RecordReader(temp_path) as reader:
            for i, record in enumerate(reader):
                print(f"Read record {i}: {record.to_bytes().decode()}")
    finally:
        # Clean up
        os.unlink(temp_path)

def multi_threaded_example():
    """Demonstrate multi-threaded API with sharding."""
    print("\nRunning multi-threaded example...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Using temporary directory: {temp_dir}")
        
        # Write records to multiple shards
        with MultiThreadedWriter.new_with_shards(
            dir_path=temp_dir,
            num_shards=3,
            worker_threads=2,
            prefix="example"
        ) as writer:
            for i in range(20):
                writer.write_record(f"MT Record {i}".encode())
            print(f"Wrote 20 records across 3 shards")
        
        # List created files
        shard_files = [f for f in os.listdir(temp_dir) if f.startswith("example")]
        print(f"Created shard files: {shard_files}")
        
        # Read from multiple shards
        with MultiThreadedReader.new_with_shards(
            dir_path=temp_dir,
            num_shards=3,
            worker_threads=2, 
            prefix="example"
        ) as reader:
            for i, record in enumerate(reader):
                if i < 5:  # Just print a few samples
                    print(f"Read MT record {i}: {record.to_bytes().decode()}")
            print(f"Read all records from shards")

if __name__ == "__main__":
    print("Pisky Consumer Example")
    print("======================")
    
    simple_example()
    multi_threaded_example()
    
    print("\nAll examples completed successfully!")