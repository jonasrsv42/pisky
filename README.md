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
from pisky import RecordWriterConfig

with RecordWriterConfig("output.disky") as writer:
    writer.write(b"Record 1")
    writer.write(b"Record 2")
    writer.write(b"Record 3")
```

### Reading Records

```python
from pisky import RecordReaderConfig

with RecordReaderConfig("input.disky") as reader:
    for record in reader:
        print(f"Record: {bytes(record).decode('utf-8')}")
```

### Writing with Compression

```python
from pisky import RecordWriterConfig, Zstd

# Write with zstd compression (level 1-22, default 3)
with RecordWriterConfig("output.disky", compression=Zstd(3)) as writer:
    writer.write(b"Compressed record 1")
    writer.write(b"Compressed record 2")
```

### Reading with Corruption Recovery

```python
from pisky import RecordReaderConfig, CorruptionStrategy

# Skip corrupted chunks instead of raising errors
with RecordReaderConfig("input.disky", CorruptionStrategy.RECOVER) as reader:
    for record in reader:
        print(bytes(record))
```

### Counting Records

```python
from pisky import RecordReaderConfig

count = RecordReaderConfig.count_records("input.disky")
print(f"File contains {count} records")
```

## Sharded Files

For large datasets, Pisky supports reading and writing to multiple shard files.

### Sequential Shard Writing

```python
from pisky.shard import writer, order

# Create a shard sink
sink = writer.FileShards.from_pattern("/data", "shard")

# Write with automatic rotation at 1GB per shard
with writer.SequentialConfig(sink, max_shard_bytes=1_000_000_000) as w:
    for i in range(100000):
        w.write(f"Record {i}".encode())
```

### Sequential Shard Reading

```python
from pisky.shard import reader, order
from pisky.shard.file_shards import FileShards

# Create shard source
shards = FileShards.from_pattern("/data", "shard")

# Read shards sequentially (one at a time)
seq = order.Sequential(shards)
with reader.SequentialConfig(seq) as r:
    for record in r:
        print(bytes(record))
```

### Round-Robin Shard Reading

```python
from pisky.shard import reader, order
from pisky.shard.file_shards import FileShards

shards = FileShards.from_pattern("/data", "shard")

# Read one record from each shard in rotation
seq = order.Sequential(shards)
with reader.RoundRobinConfig(seq, max_active=4) as r:
    for record in r:
        print(bytes(record))
```

### Infinite Random Reading (for ML training)

```python
from pisky.shard import reader, order
from pisky.shard.file_shards import FileShards

shards = FileShards.from_pattern("/data", "shard")

# Read shards in random order, repeating forever
rand = order.RandomRepeat(shards)
with reader.SequentialConfig(rand) as r:
    for i, record in enumerate(r):
        if i >= 10000:  # Stop after 10k records
            break
        process(bytes(record))
```

## Multi-Threaded API

For high-throughput scenarios, Pisky provides a multi-threaded API that reads/writes in parallel using worker threads.

### Multi-Threaded Writing

```python
from pisky.multi_threaded import writer
from pisky import Zstd

sink = writer.FileShards.from_pattern("/data", "shard")

with writer.MultiThreadedConfig(
    sink,
    num_shards=4,           # Number of concurrent shards
    compression=Zstd(3),    # Optional compression
) as w:
    for i in range(100000):
        w.write(f"Record {i}".encode())
```

### Multi-Threaded Reading

```python
from pisky.multi_threaded import reader
from pisky.shard import order
from pisky.shard.file_shards import FileShards

shards = FileShards.from_pattern("/data", "shard")

# Sequential order (finite)
seq = order.Sequential(shards)
with reader.MultiThreadedConfig(seq, num_parallel=4, worker_threads=2) as r:
    for record in r:
        process(bytes(record))

# Random repeating order (infinite, for ML training)
rand = order.RandomRepeat(shards)
with reader.MultiThreadedConfig(rand, num_parallel=4) as r:
    for i, record in enumerate(r):
        if i >= 100000:
            break
        process(bytes(record))
```

## API Reference

### Compression Types

- `Zstd(level=3)` - Zstandard compression (level 1-22)
- `Uncompressed()` - No compression

### Corruption Strategies

- `CorruptionStrategy.ERROR` - Raise error on corruption (default)
- `CorruptionStrategy.RECOVER` - Skip corrupted chunks and continue

### Shard Orders

- `order.Sequential(shards)` - Read shards in order, stops at end
- `order.RandomRepeat(shards)` - Read shards randomly, repeats forever

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v
```

## License

Licensed under the Apache License, Version 2.0.
