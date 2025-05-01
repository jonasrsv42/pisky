"""
Example of using the Pisky MultiThreadedWriter and MultiThreadedReader.

This module demonstrates how to write and read records using 
the multi-threaded implementations in Pisky.
"""

import os
import tempfile
from pisky import MultiThreadedWriter, MultiThreadedReader
import time

def multi_threaded_write_read_example():
    """
    Example of writing records using multi-threaded writer and then reading them back.
    
    This demonstrates the basic write-then-read workflow with multiple shards.
    """
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Created temporary directory: {temp_dir}")
        
        # Number of records to write
        num_records = 10000
        
        # Write records with the multi-threaded writer
        start_time = time.time()
        with MultiThreadedWriter.new_with_shards(
            dir_path=temp_dir,
            # Using default values for other parameters
        ) as writer:
            print(f"Writing {num_records} records using multi-threaded writer...")
            for i in range(num_records):
                data = f"Multi-threaded Record #{i}".encode('utf-8')
                writer.write_record(data)
                
                if i % 1000 == 0 and i > 0:
                    print(f"Wrote {i} records...")
                    
        write_time = time.time() - start_time
        print(f"Finished writing in {write_time:.2f} seconds")
        
        # List the created shard files
        shard_files = [f for f in os.listdir(temp_dir) if f.startswith("shard_")]
        print(f"Created {len(shard_files)} shard files: {shard_files}")
        
        # Read records with the multi-threaded reader
        start_time = time.time()
        print(f"Reading records using multi-threaded reader...")
        with MultiThreadedReader.new_with_shards(
            dir_path=temp_dir,
            # Using default values for other parameters
        ) as reader:
            # Use the iterator interface
            count = 0
            for record in reader:
                count += 1
                if count % 1000 == 0:
                    print(f"Read {count} records...")
                
                # Do something with the record (just converting to string in this example)
                if count <= 3:  # Print only the first few records
                    print(f"Sample record: {record.to_bytes().decode('utf-8')}")
                    
        read_time = time.time() - start_time
        print(f"Finished reading {count} records in {read_time:.2f} seconds")

def custom_settings_example():
    """
    Example with more custom settings for the multi-threaded reader and writer.
    """
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"\nCreated temporary directory for custom settings: {temp_dir}")
        
        # Write records with custom settings
        with MultiThreadedWriter.new_with_shards(
            dir_path=temp_dir,
            prefix="custom",
            num_shards=4,
            worker_threads=2,
            max_bytes_per_writer=100 * 1024 * 1024,  # 100 MB per writer
            task_queue_capacity=500,
            enable_auto_sharding=True,
            append=False
        ) as writer:
            print("Writing records with custom settings...")
            for i in range(1000):
                data = f"Custom Record #{i}".encode('utf-8')
                writer.write_record(data)
                
        # List the created shard files
        shard_files = [f for f in os.listdir(temp_dir) if f.startswith("custom_")]
        print(f"Created {len(shard_files)} custom shard files: {shard_files}")
        
        # Read records with custom settings
        with MultiThreadedReader.new_with_shards(
            dir_path=temp_dir,
            prefix="custom",
            num_shards=4,
            worker_threads=2,
            queue_size_mb=100  # 100 MB queue
        ) as reader:
            print("Reading records with custom settings...")
            count = 0
            for record in reader:
                count += 1
                if count <= 3:  # Print only the first few records
                    print(f"Sample custom record: {record.to_bytes().decode('utf-8')}")
                    
        print(f"Read {count} records with custom settings")

if __name__ == "__main__":
    print("=== Multi-threaded Write and Read Example ===")
    multi_threaded_write_read_example()
    
    print("\n=== Custom Settings Example ===")
    custom_settings_example()