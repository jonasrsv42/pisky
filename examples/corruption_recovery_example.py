"""
Example demonstrating the corruption recovery feature in Pisky.

This example shows how to use the CorruptionStrategy to handle corrupted files.
"""

import os
import tempfile
from pisky import (
    RecordWriter, 
    RecordReader, 
    MultiThreadedWriter, 
    MultiThreadedReader, 
    CorruptionStrategy, 
    set_log_level
)

def simple_corruption_recovery_example():
    """
    Example of using the CorruptionStrategy.RECOVER option with RecordReader.
    
    Note that corruption recovery operates at the chunk level - if corruption is detected,
    the entire chunk (typically about 1MB of data) will be skipped, not just individual records.
    """
    # Create a temporary file
    with tempfile.NamedTemporaryFile(suffix=".disky", delete=False) as temp:
        temp_path = temp.name
    
    try:
        # Write many records to ensure multiple chunks (each chunk is ~1MB)
        # We'll aim for at least 2MB of data
        print(f"Writing records to {temp_path}")
        with RecordWriter(temp_path) as writer:
            # Create a 1KB record - repeated to create >2MB
            record_data = b"X" * 1024  # 1KB of data
            
            # Write 2000 records (should be around 2MB total)
            for i in range(2000):
                writer.write_record(f"Record #{i}: ".encode('utf-8') + record_data)
        
        file_size = os.path.getsize(temp_path)
        print(f"Created file of size: {file_size / (1024*1024):.2f} MB")
        
        # Simulate corruption by inserting invalid data at byte position ~400
        # This should corrupt the first chunk but leave later chunks intact
        with open(temp_path, 'r+b') as f:
            f.seek(400)  # Seek to position 400
            f.write(b"CORRUPTION" * 50)  # Insert corruption
        
        print(f"Added corruption at position 400")
        
        # Try to read with default error strategy (will fail)
        print("\nTrying to read with default error strategy (will fail):")
        try:
            records_read = 0
            with RecordReader(temp_path) as reader:
                for record in reader:
                    records_read += 1
                    if records_read <= 3:
                        print(f"Record {records_read}: {record.to_bytes()[:30]}...")
            print(f"Read {records_read} records successfully")
        except Exception as e:
            print(f"Error reading file: {e}")
            print(f"Read {records_read} records before error")
        
        # Try to read with recovery strategy
        print("\nTrying to read with recovery strategy:")
        with RecordReader(temp_path, corruption_strategy=CorruptionStrategy.RECOVER) as reader:
            record_count = 0
            for record in reader:
                record_count += 1
                if record_count <= 3:
                    print(f"Record {record_count}: {record.to_bytes()[:30]}...")
            
            print(f"Successfully read {record_count} records with recovery strategy")
            print(f"Note: Some records may be lost because entire chunks containing corruption are skipped")
    
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_path):
            os.unlink(temp_path)
            print(f"\nDeleted temporary file: {temp_path}")

def multithreaded_corruption_recovery_example():
    """
    Example of using the CorruptionStrategy.RECOVER option with MultiThreadedReader.
    
    This demonstrates how to use the corruption strategy with the multi-threaded reader.
    """
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"\nCreated temporary directory: {temp_dir}")
        
        # Write records to multiple shards, ensuring each shard has multiple chunks
        with MultiThreadedWriter.new_with_shards(
            dir_path=temp_dir,
            prefix="shard",
            num_shards=2
        ) as writer:
            print("Writing records to sharded files (2 shards, multiple chunks each)")
            
            # Create a 1KB record - repeated to create >2MB per shard
            record_data = b"X" * 1024  # 1KB of data
            
            # Write 4000 records (should be around 2MB per shard with 2 shards)
            for i in range(4000):
                writer.write_record(f"Record #{i}: ".encode('utf-8') + record_data)
                if i % 1000 == 0 and i > 0:
                    print(f"Wrote {i} records...")
        
        # List the created shard files
        shard_files = sorted([os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.startswith("shard")])
        print(f"Created {len(shard_files)} shard files")
        
        # Show shard sizes
        for shard_file in shard_files:
            size_mb = os.path.getsize(shard_file) / (1024*1024)
            print(f"Shard {os.path.basename(shard_file)}: {size_mb:.2f} MB")
        
        # Corrupt one of the shards at position ~400
        if shard_files:
            with open(shard_files[0], 'r+b') as f:
                f.seek(400)  # Seek to position 400
                f.write(b"CORRUPTION" * 50)  # Insert corruption
            print(f"Corrupted shard file at position 400: {shard_files[0]}")
        
        # Try to read with default error strategy (will fail)
        print("\nTrying to read with default error strategy (will fail):")
        try:
            records_read = 0
            with MultiThreadedReader.new_with_shards(
                dir_path=temp_dir,
                prefix="shard",
                num_shards=2
            ) as reader:
                for record in reader:
                    records_read += 1
                    if records_read <= 3:
                        print(f"Record {records_read}: {record.to_bytes()[:30]}...")
            print(f"Read {records_read} records successfully")
        except Exception as e:
            print(f"Error reading files: {e}")
            print(f"Read {records_read} records before error")
        
        # Try to read with recovery strategy
        print("\nTrying to read with recovery strategy:")
        with MultiThreadedReader.new_with_shards(
            dir_path=temp_dir,
            prefix="shard",
            num_shards=2,
            corruption_strategy=CorruptionStrategy.RECOVER
        ) as reader:
            record_count = 0
            for record in reader:
                record_count += 1
                if record_count <= 3:
                    print(f"Record {record_count}: {record.to_bytes()[:30]}...")
            
            print(f"Successfully read {record_count} records with recovery strategy")
            print(f"Note: Some records may be lost because entire chunks containing corruption are skipped")

if __name__ == "__main__":
    # Set logging level to debug to see detailed information about corruption handling
    print("Setting log level to debug for detailed corruption handling information...")
    set_log_level("debug")
    
    print("=== Simple Corruption Recovery Example ===")
    simple_corruption_recovery_example()
    
    print("\n=== Multi-threaded Corruption Recovery Example ===")
    multithreaded_corruption_recovery_example()