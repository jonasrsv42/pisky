# Pisky: Python Bindings for Disky

Pisky provides Python bindings for the [Disky](https://github.com/jonasrsv42/disky) library. These bindings allow Python developers to efficiently read and write record-based data using the Disky format.

## Installation

### From source

1. Clone the repository:
```bash
git clone https://github.com/jonasrsv42/vibe-disky.git
cd vibe-disky/pisky
```

2. Create a virtual environment and install development dependencies:
```bash
python -m venv .venv
source .venv/bin/activate
pip install maturin
```

3. Build and install in development mode:
```bash
maturin develop
```

## Quick Start

### Writing Records

```python
from pisky import RecordWriter

# Write records to a file
with RecordWriter("output.disky") as writer:
    writer.write_record(b"Record 1")
    writer.write_record(b"Record 2")
    writer.write_record(b"Record 3")
```

### Reading Records

```python
from pisky import RecordReader

# Read records from a file
with RecordReader("input.disky") as reader:
    for record in reader:
        # Note: record is a custom Bytes object with zero-copy semantics
        # Use to_bytes() to convert to a standard Python bytes object
        python_bytes = record.to_bytes()
        print(f"Record: {python_bytes.decode('utf-8')}")
```

### Writing Records from a List

```python
from pisky import RecordWriter

# List of records to write
records = [
    b"Record A",
    b"Record B",
    b"Record C",
]

# Write all records to a file
with RecordWriter("output.disky") as writer:
    for record in records:
        writer.write_record(record)
```

### Manual Reading and Writing

If you prefer not to use a context manager, you can manually manage the resources:

```python
from pisky import RecordWriter, RecordReader

# Writing manually
writer = RecordWriter("output.disky")
try:
    writer.write_record(b"Record 1")
    writer.write_record(b"Record 2")
    writer.flush()  # Flush data to disk
finally:
    writer.close()  # Always close the writer

# Reading manually
reader = RecordReader("output.disky")
try:
    while True:
        record = reader.next_record()
        if record is None:
            break  # End of file
        print(record.to_bytes().decode('utf-8'))
finally:
    reader.close()  # Explicitly close the reader
```

## Advanced Usage: Multi-Threaded API

Pisky also provides a multi-threaded API for high-throughput scenarios. This API allows for parallel reading and writing of records across multiple shards.

### Multi-Threaded Writing

```python
from pisky import MultiThreadedWriter
import tempfile
import os

with tempfile.TemporaryDirectory() as temp_dir:
    # Multi-threaded writing with default settings
    with MultiThreadedWriter.new_with_shards(dir_path=temp_dir) as writer:
        for i in range(10000):
            writer.write_record(f"Record #{i}".encode('utf-8'))
    
    # List the created shard files 
    shard_files = [f for f in os.listdir(temp_dir) if f.startswith("shard_")]
    print(f"Created {len(shard_files)} shard files")
```

### Multi-Threaded Reading

```python
from pisky import MultiThreadedReader
import tempfile

with tempfile.TemporaryDirectory() as temp_dir:
    # Assuming temp_dir contains sharded files
    # Read using multi-threaded reader
    with MultiThreadedReader.new_with_shards(dir_path=temp_dir) as reader:
        count = 0
        for record in reader:
            count += 1
            if count <= 5:  # Print just a few samples
                print(f"Sample: {record.to_bytes().decode('utf-8')}")
        print(f"Read {count} records")
```

### Custom Multi-Threaded Configuration

```python
from pisky import MultiThreadedWriter, MultiThreadedReader
import tempfile

with tempfile.TemporaryDirectory() as temp_dir:
    # Custom configuration for writer
    with MultiThreadedWriter.new_with_shards(
        dir_path=temp_dir,
        prefix="custom",         # Custom file prefix (default: "shard")
        num_shards=4,            # Number of shard files to create (default: 2)
        worker_threads=8,        # Number of worker threads (default: auto)
        task_queue_capacity=5000,# Task queue capacity (default: 2000)
        enable_auto_sharding=True,# Enable auto-sharding (default: True)
        append=False,            # Append mode (default: True)
    ) as writer:
        for i in range(1000):
            writer.write_record(f"Record #{i}".encode('utf-8'))
    
    # Custom configuration for reader
    with MultiThreadedReader.new_with_shards(
        dir_path=temp_dir,
        prefix="custom",         # Must match the writer's prefix
        num_shards=4,            # Must match the writer's num_shards
        worker_threads=4,        # Number of worker threads (default: 1)
        queue_size_mb=1024,      # Queue size in MB (default: 10GB)
    ) as reader:
        for record in reader:
            # Process records...
            pass
```

## API Reference

### Single-Threaded API

#### `RecordWriter`

The main class for writing records to Disky files.

- **Constructor**: `RecordWriter(path: str)`: Create a new writer that writes to the specified file path.
- **Methods**:
  - `write_record(data: bytes) -> None`: Write a record to the file.
  - `flush() -> None`: Flush any buffered records to disk.
  - `close() -> None`: Close the writer, flushing any remaining data.

#### `RecordReader`

The main class for reading records from Disky files.

- **Constructor**: `RecordReader(path: str)`: Create a new reader that reads from the specified file path.
- **Methods**:
  - `next_record() -> Optional[Bytes]`: Read the next record from the file, or None at EOF.
  - `close() -> None`: Close the reader.

#### `Bytes`

A custom bytes-like class returned by the reader, with zero-copy semantics.

- **Methods**:
  - `to_bytes() -> bytes`: Convert to a standard Python bytes object.
  - Plus most standard bytes methods like `isalnum()`, `upper()`, etc.

### Multi-Threaded API

#### `MultiThreadedWriter`

The class for writing records across multiple shards in parallel.

- **Constructor**: 
  - `MultiThreadedWriter.new_with_shards(dir_path: str, prefix: str = "shard", num_shards: int = 2, worker_threads: Optional[int] = None, max_bytes_per_writer: Optional[int] = 10GB, task_queue_capacity: int = 2000, enable_auto_sharding: bool = True, append: bool = True)`: Create a new multi-threaded writer.
  
- **Methods**:
  - `write_record(data: bytes) -> None`: Write a record to a shard file.
  - `flush() -> None`: Flush any buffered records to disk.
  - `close() -> None`: Close the writer, flushing any remaining data.
  - `pending_tasks() -> int`: Get the number of pending write tasks.
  - `available_writers() -> int`: Get the number of available writer resources.

#### `MultiThreadedReader`

The class for reading records from multiple shards in parallel.

- **Constructor**: 
  - `MultiThreadedReader.new_with_shards(dir_path: str, prefix: str = "shard", num_shards: int = 2, worker_threads: int = 1, queue_size_mb: int = 10*1024)`: Create a new multi-threaded reader.
  
- **Methods**:
  - `next_record() -> Optional[Bytes]`: Read the next record, or None at EOF.
  - `close() -> None`: Close the reader.
  - `queued_records() -> int`: Get the number of records currently in the queue.
  - `queued_bytes() -> int`: Get the total size of queued records in bytes.

## Development

### Setting up Development Environment

```bash
# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dev dependencies
pip install -e ".[dev]"

# Build the extension in development mode
maturin develop
```

### Type Checking

Pisky includes type annotations and supports static type checking with mypy. When using Pisky in your project, mypy will automatically recognize and validate the types.

```bash
# Install mypy
pip install mypy

# Run mypy on your project that uses pisky
mypy your_project.py
```

### Running Tests

```bash
# Run all tests
python -m pytest

# Run with verbose output
python -m pytest -v

# Run a specific test file
python -m pytest tests/test_pisky.py
```

## License

Licensed under the Apache License, Version 2.0.
