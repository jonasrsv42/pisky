# Pisky: Python Bindings for Disky

Pisky provides Python bindings for the [Disky](https://github.com/jonasrsv42/disky) library, a high-performance Rust implementation of the Riegeli file format. These bindings allow Python developers to efficiently write record-based data using the Disky format.

## Installation

To install pisky, you need to have Rust and Python installed. Then you can build and install the package using:

```bash
# Navigate to the pisky directory
cd pisky

# Install with pip
pip install -e .
```

This will build the Rust extension module and install it for development use.

## Usage

### Basic Example

```python
from pisky import RecordWriter

# Write records to a file
with RecordWriter("output.disky") as writer:
    writer.write_record(b"Record 1")
    writer.write_record(b"Record 2")
    writer.write_record(b"Record 3")
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

### Manual Resource Management

If you prefer not to use a context manager, you can manually manage the writer:

```python
from pisky import RecordWriter

writer = RecordWriter("output.disky")

try:
    writer.write_record(b"Record 1")
    writer.write_record(b"Record 2")
    writer.flush()  # Flush data to disk
finally:
    writer.close()  # Always close the writer
```

## API Reference

### `RecordWriter`

The main class for writing records to Disky files.

#### Constructor

- `RecordWriter(path: str)`: Create a new writer that writes to the specified file path.

#### Methods

- `write_record(data: bytes) -> None`: Write a record to the file.
- `flush() -> None`: Flush any buffered records to disk.
- `close() -> None`: Close the writer, flushing any remaining data.

#### Context Manager

`RecordWriter` supports the context manager protocol (`with` statement), which automatically closes the writer when exiting the context.

## Building from Source

To build the project from source:

```bash
# Clone the repository
git clone https://github.com/yourusername/vibe-disky.git
cd vibe-disky/pisky

# Build the extension module
maturin develop
```

## Running the Examples

```bash
# Navigate to the pisky package directory
cd pisky/pisky

# Run the writer example
python writer.py
```

## License

Licensed under the Apache License, Version 2.0.
