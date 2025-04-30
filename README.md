# Pisky: Python Bindings for Disky

Pisky provides Python bindings for the [Disky](https://github.com/jonasrsv42/disky) library, a high-performance Rust implementation of the Riegeli file format. These bindings allow Python developers to efficiently read and write record-based data using the Disky format.

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

### `RecordReader`

The main class for reading records from Disky files.

#### Constructor

- `RecordReader(path: str)`: Create a new reader that reads from the specified file path.

#### Methods

- `next_record() -> Optional[Bytes]`: Read the next record from the file, or None at EOF.
- `close() -> None`: Close the reader.

#### Context Manager and Iterator

`RecordReader` supports both the context manager protocol (`with` statement) and the iterator protocol (for loops), making it easy to read records.

### `Bytes`

A custom bytes-like class returned by the reader, with zero-copy semantics.

#### Methods

- `to_bytes() -> bytes`: Convert to a standard Python bytes object.
- Plus most standard bytes methods like `isalnum()`, `upper()`, etc.

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
# Navigate to the pisky examples directory
cd pisky/examples

# Run the read/write example
python read_write_example.py
```

## Performance Considerations

The Python bindings for Disky use zero-copy mechanisms when reading records, providing excellent performance even for large datasets. When processing large files, the memory usage is kept minimal as records are read on demand rather than all at once.

## License

Licensed under the Apache License, Version 2.0.