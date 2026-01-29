#!/usr/bin/env python
"""
Test script to verify installation and basic functionality of pisky from the GitHub release.
"""
import os
import tempfile

try:
    # Try to import from the installed pisky package
    from pisky import RecordWriterConfig, RecordReaderConfig, Zstd

    print("✅ Successfully imported pisky module")

    # Test basic write and read functionality
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        temp_path = tmp.name

    # Write some records
    with RecordWriterConfig(temp_path) as writer:
        writer.write(b"Test record 1")
        writer.write(b"Test record 2")
        writer.write(b"Test record 3")
        print("✅ Successfully wrote records")

    # Read the records back
    with RecordReaderConfig(temp_path) as reader:
        records = [bytes(record) for record in reader]
        print("✅ Successfully read records:", records)

    print("✅ Successfully counted records:", RecordReaderConfig.count_records(temp_path))

    # Test zstd compression functionality (new in v0.6.0)
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        zstd_temp_path = tmp.name

    # Test zstd compression
    with RecordWriterConfig(zstd_temp_path, compression=Zstd(3)) as writer:
        writer.write(b"Compressed record 1")
        writer.write(b"Compressed record 2" * 100)  # Repetitive data
        print("✅ Successfully wrote records with zstd compression")

    # Read back compressed records
    with RecordReaderConfig(zstd_temp_path) as reader:
        compressed_records = [bytes(record) for record in reader]
        assert len(compressed_records) == 2
        assert compressed_records[0] == b"Compressed record 1"
        assert compressed_records[1] == b"Compressed record 2" * 100
        print("✅ Successfully read zstd compressed records")
    
    # Clean up
    os.unlink(temp_path)
    os.unlink(zstd_temp_path)
    print("✅ All tests passed! Pisky installation verified with zstd compression support.")

except ImportError as e:
    print(f"❌ Failed to import pisky: {e}")
    print("Make sure pisky is installed correctly.")
except Exception as e:
    print(f"❌ Test failed: {e}")
