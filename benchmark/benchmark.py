#!/usr/bin/env python
"""
Benchmark comparing Pisky with Google's Array Record format for reading and writing performance.
Measures throughput in MB/s for various data sizes.
"""

import os
import time
import tempfile
import shutil
import numpy as np
from tabulate import tabulate
from tqdm import tqdm
from array_record.python import array_record_module as ar
import pisky

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

def generate_data(size_bytes, count):
    """Generate random binary data for benchmarking."""
    return [np.random.bytes(size_bytes) for _ in range(count)]

def benchmark_pisky_write(data, dir_path=None):
    """Benchmark Pisky writing performance."""
    if dir_path is None:
        temp_dir = tempfile.mkdtemp()
        try:
            return benchmark_pisky_write(data, temp_dir)
        finally:
            shutil.rmtree(temp_dir)
    
    file_path = os.path.join(dir_path, "pisky_benchmark.disky")
    
    start_time = time.time()
    with pisky.RecordWriter(file_path) as writer:
        for record in data:
            writer.write_record(record)
    end_time = time.time()
    
    elapsed_time = end_time - start_time
    total_mb = sum(len(record) for record in data) / (1024 * 1024)  # Convert to MB
    throughput = total_mb / elapsed_time  # MB/s
    
    return {
        "throughput_mb_s": throughput,
        "elapsed_time": elapsed_time,
        "file_path": file_path,
        "file_size_mb": os.path.getsize(file_path) / (1024 * 1024)
    }

def benchmark_pisky_read(file_path):
    """Benchmark Pisky reading performance."""
    start_time = time.time()
    record_count = 0
    total_bytes = 0
    
    with pisky.RecordReader(file_path) as reader:
        for record in reader:
            record_bytes = record.to_bytes()
            total_bytes += len(record_bytes)
            record_count += 1
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    total_mb = total_bytes / (1024 * 1024)  # Convert to MB
    throughput = total_mb / elapsed_time  # MB/s
    
    return {
        "throughput_mb_s": throughput,
        "elapsed_time": elapsed_time,
        "record_count": record_count,
        "total_mb": total_mb
    }

def benchmark_array_record_write(data, dir_path=None):
    """Benchmark Array Record writing performance."""
    if dir_path is None:
        temp_dir = tempfile.mkdtemp()
        try:
            return benchmark_array_record_write(data, temp_dir)
        finally:
            shutil.rmtree(temp_dir)
    
    file_path = os.path.join(dir_path, "array_record_benchmark.arrayrec")
    
    start_time = time.time()
    writer = ar.ArrayRecordWriter(file_path)
    try:
        for record in data:
            writer.write(record)
        writer.close()
    except Exception as e:
        writer.close()
        raise e
    end_time = time.time()
    
    elapsed_time = end_time - start_time
    total_mb = sum(len(record) for record in data) / (1024 * 1024)  # Convert to MB
    throughput = total_mb / elapsed_time  # MB/s
    
    return {
        "throughput_mb_s": throughput,
        "elapsed_time": elapsed_time,
        "file_path": file_path,
        "file_size_mb": os.path.getsize(file_path) / (1024 * 1024)
    }

def benchmark_array_record_read(file_path):
    """Benchmark Array Record reading performance."""
    start_time = time.time()
    reader = ar.ArrayRecordReader(file_path)
    try:
        # Get total number of records
        num_records = reader.num_records()
        total_bytes = 0
        
        # Read all records one by one
        for i in range(num_records):
            record = reader.read()  # Read the next record
            total_bytes += len(record)
        
        reader.close()
    except Exception as e:
        reader.close()
        raise e
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    total_mb = total_bytes / (1024 * 1024)  # Convert to MB
    throughput = total_mb / elapsed_time  # MB/s
    
    return {
        "throughput_mb_s": throughput,
        "elapsed_time": elapsed_time,
        "record_count": num_records,
        "total_mb": total_mb
    }

def run_benchmark():
    """Run the complete benchmark suite."""
    results = []
    
    with tempfile.TemporaryDirectory() as temp_dir:
        for record_size in RECORD_SIZES:
            num_records = RECORDS_BY_SIZE[record_size]
            size_display = f"{record_size / (1024*1024):.1f} MB" if record_size >= 1024*1024 else f"{record_size / 1024:.1f} KB"
            total_size = (record_size * num_records) / (1024*1024)
            
            print(f"\nBenchmarking with record size: {size_display} × {num_records} records (total ~{total_size:.1f} MB)")
            
            # Results for this record size
            pisky_write_results = []
            pisky_read_results = []
            array_record_write_results = []
            array_record_read_results = []
            
            for i in range(REPEAT_TIMES):
                print(f"  Run {i+1}/{REPEAT_TIMES}")
                
                # Generate test data
                print(f"  Generating {num_records} records of {size_display} each...")
                data = generate_data(record_size, num_records)
                
                # Benchmark Pisky Write
                print("  Benchmarking Pisky Write...")
                pisky_write_result = benchmark_pisky_write(data, temp_dir)
                pisky_write_results.append(pisky_write_result)
                pisky_file_path = pisky_write_result["file_path"]
                
                # Benchmark Pisky Read
                print("  Benchmarking Pisky Read...")
                pisky_read_result = benchmark_pisky_read(pisky_file_path)
                pisky_read_results.append(pisky_read_result)
                
                # Benchmark Array Record Write
                print("  Benchmarking Array Record Write...")
                array_record_write_result = benchmark_array_record_write(data, temp_dir)
                array_record_write_results.append(array_record_write_result)
                array_record_file_path = array_record_write_result["file_path"]
                
                # Benchmark Array Record Read
                print("  Benchmarking Array Record Read...")
                array_record_read_result = benchmark_array_record_read(array_record_file_path)
                array_record_read_results.append(array_record_read_result)
            
            # Calculate averages
            avg_pisky_write = sum(result["throughput_mb_s"] for result in pisky_write_results) / REPEAT_TIMES
            avg_pisky_read = sum(result["throughput_mb_s"] for result in pisky_read_results) / REPEAT_TIMES
            avg_array_record_write = sum(result["throughput_mb_s"] for result in array_record_write_results) / REPEAT_TIMES
            avg_array_record_read = sum(result["throughput_mb_s"] for result in array_record_read_results) / REPEAT_TIMES
            
            avg_pisky_file_size = sum(result["file_size_mb"] for result in pisky_write_results) / REPEAT_TIMES
            avg_array_record_file_size = sum(result["file_size_mb"] for result in array_record_write_results) / REPEAT_TIMES
            
            # Add to results
            results.append({
                "record_size_kb": record_size / 1024,
                "pisky_write_mb_s": avg_pisky_write,
                "pisky_read_mb_s": avg_pisky_read,
                "array_record_write_mb_s": avg_array_record_write,
                "array_record_read_mb_s": avg_array_record_read,
                "pisky_file_size_mb": avg_pisky_file_size,
                "array_record_file_size_mb": avg_array_record_file_size,
            })
    
    # Print results table
    table_data = []
    headers = [
        "Record Size", 
        "Pisky Write (MB/s)", 
        "Pisky Read (MB/s)", 
        "AR Write (MB/s)", 
        "AR Read (MB/s)",
        "Pisky Size (MB)",
        "AR Size (MB)"
    ]
    
    for result in results:
        # Format record size appropriately (KB or MB)
        if result['record_size_kb'] >= 1024:
            size_str = f"{result['record_size_kb']/1024:.1f} MB"
        else:
            size_str = f"{result['record_size_kb']:.1f} KB"
            
        table_data.append([
            size_str,
            f"{result['pisky_write_mb_s']:.2f}",
            f"{result['pisky_read_mb_s']:.2f}",
            f"{result['array_record_write_mb_s']:.2f}",
            f"{result['array_record_read_mb_s']:.2f}",
            f"{result['pisky_file_size_mb']:.2f}",
            f"{result['array_record_file_size_mb']:.2f}"
        ])
    
    print("\nBenchmark Results:")
    print(tabulate(table_data, headers=headers, tablefmt="grid"))
    
    # Print performance comparison
    print("\nPerformance Comparison (Pisky vs Array Record):")
    for result in results:
        print(f"\nRecord Size: {result['record_size_kb']:.1f} KB")
        write_ratio = result['pisky_write_mb_s'] / result['array_record_write_mb_s']
        read_ratio = result['pisky_read_mb_s'] / result['array_record_read_mb_s']
        size_ratio = result['array_record_file_size_mb'] / result['pisky_file_size_mb']
        
        print(f"  Write Speed: Pisky is {write_ratio:.2f}x {'faster' if write_ratio > 1 else 'slower'}")
        print(f"  Read Speed: Pisky is {read_ratio:.2f}x {'faster' if read_ratio > 1 else 'slower'}")
        print(f"  File Size: Pisky files are {size_ratio:.2f}x {'smaller' if size_ratio > 1 else 'larger'}")

if __name__ == "__main__":
    pisky.set_log_level("warn")  # Reduce log output for cleaner benchmark output
    run_benchmark()