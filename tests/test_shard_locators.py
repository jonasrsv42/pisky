"""
Tests for the new shard locators in pisky.

These tests verify that:
1. The MultiPathShardLocator works correctly
2. The RandomMultiPathShardLocator works correctly with random reading and repeating
"""

import os
import tempfile
import pytest
from pisky import RecordWriter, MultiThreadedWriter, MultiThreadedReader


class TestShardLocators:
    """Tests for the new shard locator implementations."""

    def setup_method(self):
        """Set up test data by creating multiple shard files."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.dir_path = self.temp_dir.name
        
        # Create shard files using the traditional writer
        with MultiThreadedWriter.new_with_shards(
            dir_path=self.dir_path,
            prefix="shard",
            num_shards=3,
        ) as writer:
            for i in range(100):
                writer.write_record(f"Record {i}".encode())
                
        # Get the list of shard paths for use in tests
        self.shard_paths = [
            os.path.join(self.dir_path, f) for f in os.listdir(self.dir_path) 
            if f.startswith("shard_")
        ]
        
        # Verify shards were created
        assert len(self.shard_paths) == 3

    def teardown_method(self):
        """Clean up temporary files."""
        self.temp_dir.cleanup()

    def test_multi_path_shard_locator(self):
        """Test reading records using the MultiPathShardLocator."""
        # Read records using explicit shard paths
        records = []
        with MultiThreadedReader.new_with_shard_paths(
            shard_paths=self.shard_paths,
            num_shards=3,
            worker_threads=2,
        ) as reader:
            for record in reader:
                records.append(record.to_bytes())
        
        # Verify all records were read
        assert len(records) == 100
        
        # Verify content of records (format should be "Record X")
        for record in records:
            assert record.startswith(b"Record ")
            
        # Verify we got all records from 0-99
        record_numbers = set()
        for record in records:
            num_str = record.decode().split(" ")[1]
            record_numbers.add(int(num_str))
            
        assert record_numbers == set(range(100))

    def test_random_multi_path_shard_locator(self):
        """Test reading records using the RandomMultiPathShardLocator."""
        # Read records with the random locator
        # This will read in random order and never reach EOF
        records = []
        with MultiThreadedReader.new_with_random_shard_paths(
            shard_paths=self.shard_paths,
            num_shards=2,
            worker_threads=2,
        ) as reader:
            # Read more records than were written to test repeating behavior
            for _ in range(500):
                record = reader.next_record()
                if record is not None:  # Should always be true for RandomMultiPathShardLocator
                    records.append(record.to_bytes())
                else:
                    # This shouldn't happen - random locator should never return None
                    assert False, "RandomMultiPathShardLocator unexpectedly reached EOF"
                    
                # As a safety measure, break if we've read 500 records
                if len(records) >= 500:
                    break
        
        # Verify we read more records than were written (confirming repeating behavior)
        assert len(records) > 100
        
        # Check that all records have the expected format
        for record in records:
            assert record.startswith(b"Record ")
            num_str = record.decode().split(" ")[1]
            num = int(num_str)
            assert 0 <= num < 100  # Valid record number
            
        # Count occurrences of each record and verify we have multiple occurrences
        # This confirms the repeating behavior
        record_counts = {}
        for record in records:
            record_str = record.decode()
            if record_str in record_counts:
                record_counts[record_str] += 1
            else:
                record_counts[record_str] = 1
                
        # Check that at least some records appear multiple times
        records_with_multiple_occurrences = [r for r, count in record_counts.items() if count > 1]
        assert len(records_with_multiple_occurrences) > 0, "No records were repeated"

    def test_partial_shard_reading(self):
        """Test reading from only a subset of available shards."""
        # Use only the first two shard files
        partial_shard_paths = self.shard_paths[:2]
        
        # Read records using the partial list of shard paths
        records = []
        with MultiThreadedReader.new_with_shard_paths(
            shard_paths=partial_shard_paths,
            num_shards=2,
        ) as reader:
            for record in reader:
                records.append(record.to_bytes())
        
        # We should have read some records but not all 100
        assert 0 < len(records) < 100, f"Expected partial records, got {len(records)}"
        
        # Verify format of the records we did read
        for record in records:
            assert record.startswith(b"Record ")
            
    def test_custom_worker_settings(self):
        """Test custom worker settings with the new shard locators."""
        # Use custom worker and queue settings
        records = []
        with MultiThreadedReader.new_with_shard_paths(
            shard_paths=self.shard_paths,
            num_shards=3,
            worker_threads=4,
            queue_size_mb=64,  # 64MB queue
        ) as reader:
            # Use the reader methods to check queue stats
            # Read some records to populate the queue
            for _ in range(10):
                reader.next_record()
                
            # Check that queue metrics work
            assert reader.queued_records() >= 0
            assert reader.queued_bytes() >= 0
            
            # Read the rest
            for record in reader:
                records.append(record.to_bytes())
        
        # Verify we read all records
        assert len(records) + 10 == 100


if __name__ == "__main__":
    pytest.main(["-v", __file__])
