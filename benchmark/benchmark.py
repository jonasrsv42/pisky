#!/usr/bin/env python
"""
Benchmark comparing Pisky with Google's Array Record format for reading and writing performance.
Measures throughput in MB/s for various data sizes.
"""

import tempfile
from pathlib import Path

import numpy as np
from tabulate import tabulate
from array_record.python import array_record_module as ar

from pisky import RecordWriterConfig, RecordReaderConfig, set_log_level

# Configure benchmark parameters
RECORD_SIZES = [1024, 10240, 102400, 5 * 1024 * 1024]  # Byte sizes: 1KB, 10KB, 100KB, 5MB
# Adjust number of records based on size to avoid excessive memory usage
RECORDS_BY_SIZE = {
    1024: 10000,        # 10K records for 1KB (about 10MB total)
    10240: 1000,        # 1K records for 10KB (about 10MB total)
    102400: 100,        # 100 records for 100KB (about 10MB total)
    5 * 1024 * 1024: 1000  # 1000 records for 5MB (about 5GB total)
}
REPEAT_TIMES = 3  # Number of times to repeat each benchmark for averaging


def generate_data(size_bytes: int, count: int) -> list[bytes]:
    """Generate random binary data for benchmarking."""
    return [np.random.bytes(size_bytes) for _ in range(count)]


def benchmark_pisky_write(data: list[bytes], temp_dir: Path) -> dict:
    """Benchmark Pisky writing performance."""
    import time

    file_path = temp_dir / "pisky_benchmark.disky"

    start_time = time.time()
    with RecordWriterConfig(file_path) as writer:
        for record in data:
            writer.write(record)
    elapsed_time = time.time() - start_time

    total_mb = sum(len(record) for record in data) / (1024 * 1024)
    throughput = total_mb / elapsed_time

    return {
        "throughput_mb_s": throughput,
        "elapsed_time": elapsed_time,
        "file_path": file_path,
        "file_size_mb": file_path.stat().st_size / (1024 * 1024)
    }


def benchmark_pisky_read(file_path: Path) -> dict:
    """Benchmark Pisky reading performance."""
    import time

    start_time = time.time()
    record_count = 0
    total_bytes = 0

    with RecordReaderConfig(file_path) as reader:
        for record in reader:
            total_bytes += len(bytes(record))
            record_count += 1

    elapsed_time = time.time() - start_time
    total_mb = total_bytes / (1024 * 1024)
    throughput = total_mb / elapsed_time

    return {
        "throughput_mb_s": throughput,
        "elapsed_time": elapsed_time,
        "record_count": record_count,
        "total_mb": total_mb
    }


def benchmark_array_record_write(data: list[bytes], temp_dir: Path) -> dict:
    """Benchmark Array Record writing performance."""
    import time

    file_path = temp_dir / "array_record_benchmark.arrayrec"

    start_time = time.time()
    writer = ar.ArrayRecordWriter(str(file_path))
    try:
        for record in data:
            writer.write(record)
        writer.close()
    except Exception as e:
        writer.close()
        raise e
    elapsed_time = time.time() - start_time

    total_mb = sum(len(record) for record in data) / (1024 * 1024)
    throughput = total_mb / elapsed_time

    return {
        "throughput_mb_s": throughput,
        "elapsed_time": elapsed_time,
        "file_path": file_path,
        "file_size_mb": file_path.stat().st_size / (1024 * 1024)
    }


def benchmark_array_record_read(file_path: Path) -> dict:
    """Benchmark Array Record reading performance."""
    import time

    start_time = time.time()
    reader = ar.ArrayRecordReader(str(file_path))
    try:
        num_records = reader.num_records()
        total_bytes = 0

        for i in range(num_records):
            record = reader.read()
            total_bytes += len(record)

        reader.close()
    except Exception as e:
        reader.close()
        raise e

    elapsed_time = time.time() - start_time
    total_mb = total_bytes / (1024 * 1024)
    throughput = total_mb / elapsed_time

    return {
        "throughput_mb_s": throughput,
        "elapsed_time": elapsed_time,
        "record_count": num_records,
        "total_mb": total_mb
    }


def format_size(size_kb: float) -> str:
    """Format size in KB to human readable string."""
    if size_kb >= 1024:
        return f"{size_kb / 1024:.1f} MB"
    return f"{size_kb:.1f} KB"


def run_benchmark():
    """Run the complete benchmark suite."""
    results = []

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)

        for record_size in RECORD_SIZES:
            num_records = RECORDS_BY_SIZE[record_size]
            size_display = format_size(record_size / 1024)
            total_size = (record_size * num_records) / (1024 * 1024)

            print(f"\nBenchmarking: {size_display} × {num_records} records (total ~{total_size:.1f} MB)")

            # Results for this record size
            pisky_write_results = []
            pisky_read_results = []
            array_record_write_results = []
            array_record_read_results = []

            for i in range(REPEAT_TIMES):
                print(f"  Run {i + 1}/{REPEAT_TIMES}")

                # Generate test data
                print(f"    Generating data...")
                data = generate_data(record_size, num_records)

                # Benchmark Pisky Write
                print("    Pisky Write...")
                pisky_write_result = benchmark_pisky_write(data, temp_dir)
                pisky_write_results.append(pisky_write_result)
                pisky_file_path = pisky_write_result["file_path"]

                # Benchmark Pisky Read
                print("    Pisky Read...")
                pisky_read_result = benchmark_pisky_read(pisky_file_path)
                pisky_read_results.append(pisky_read_result)

                # Benchmark Array Record Write
                print("    Array Record Write...")
                array_record_write_result = benchmark_array_record_write(data, temp_dir)
                array_record_write_results.append(array_record_write_result)
                array_record_file_path = array_record_write_result["file_path"]

                # Benchmark Array Record Read
                print("    Array Record Read...")
                array_record_read_result = benchmark_array_record_read(array_record_file_path)
                array_record_read_results.append(array_record_read_result)

            # Calculate averages
            avg_pisky_write = sum(r["throughput_mb_s"] for r in pisky_write_results) / REPEAT_TIMES
            avg_pisky_read = sum(r["throughput_mb_s"] for r in pisky_read_results) / REPEAT_TIMES
            avg_ar_write = sum(r["throughput_mb_s"] for r in array_record_write_results) / REPEAT_TIMES
            avg_ar_read = sum(r["throughput_mb_s"] for r in array_record_read_results) / REPEAT_TIMES

            avg_pisky_file_size = sum(r["file_size_mb"] for r in pisky_write_results) / REPEAT_TIMES
            avg_ar_file_size = sum(r["file_size_mb"] for r in array_record_write_results) / REPEAT_TIMES

            results.append({
                "record_size_kb": record_size / 1024,
                "pisky_write_mb_s": avg_pisky_write,
                "pisky_read_mb_s": avg_pisky_read,
                "array_record_write_mb_s": avg_ar_write,
                "array_record_read_mb_s": avg_ar_read,
                "pisky_file_size_mb": avg_pisky_file_size,
                "array_record_file_size_mb": avg_ar_file_size,
            })

    # Print results table
    headers = [
        "Record Size",
        "Pisky Write (MB/s)",
        "Pisky Read (MB/s)",
        "AR Write (MB/s)",
        "AR Read (MB/s)",
        "Pisky Size (MB)",
        "AR Size (MB)"
    ]

    table_data = []
    for result in results:
        table_data.append([
            format_size(result["record_size_kb"]),
            f"{result['pisky_write_mb_s']:.2f}",
            f"{result['pisky_read_mb_s']:.2f}",
            f"{result['array_record_write_mb_s']:.2f}",
            f"{result['array_record_read_mb_s']:.2f}",
            f"{result['pisky_file_size_mb']:.2f}",
            f"{result['array_record_file_size_mb']:.2f}"
        ])

    print("\n" + "=" * 80)
    print("Benchmark Results:")
    print("=" * 80)
    print(tabulate(table_data, headers=headers, tablefmt="grid"))

    # Print performance comparison
    print("\n" + "=" * 80)
    print("Performance Comparison (Pisky vs Array Record):")
    print("=" * 80)
    for result in results:
        size_str = format_size(result["record_size_kb"])
        write_ratio = result["pisky_write_mb_s"] / result["array_record_write_mb_s"]
        read_ratio = result["pisky_read_mb_s"] / result["array_record_read_mb_s"]
        size_ratio = result["array_record_file_size_mb"] / result["pisky_file_size_mb"]

        print(f"\n{size_str} records:")
        print(f"  Write: Pisky is {write_ratio:.2f}x {'faster' if write_ratio > 1 else 'slower'}")
        print(f"  Read:  Pisky is {read_ratio:.2f}x {'faster' if read_ratio > 1 else 'slower'}")
        print(f"  Size:  Pisky files are {size_ratio:.2f}x {'smaller' if size_ratio > 1 else 'larger'}")


if __name__ == "__main__":
    set_log_level("warn")
    run_benchmark()
