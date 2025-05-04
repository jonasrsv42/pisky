# Pisky vs Array Record Performance Benchmark

This benchmark compares the performance of Pisky (v0.2.0) with Google's Array Record format for reading and writing data.

## Benchmark Results

### Summary
Pisky consistently outperforms Array Record across all tested record sizes:
- **Write speed**: 1.5-2x faster
- **Read speed**: 2-4x faster 
- **File size**: Comparable (slightly smaller)

### Detailed Results

| Record Size | Pisky Write (MB/s) | Pisky Read (MB/s) | AR Write (MB/s) | AR Read (MB/s) | Pisky Size (MB) | AR Size (MB) |
|-------------|-------------------:|------------------:|----------------:|---------------:|----------------:|-------------:|
| 1.0 KB      |            1007.85 |           1405.65 |          520.79 |         623.60 |            9.79 |         9.88 |
| 10.0 KB     |             992.22 |           3215.45 |          650.12 |        1099.41 |            9.77 |         9.88 |
| 100.0 KB    |            1133.82 |           3016.96 |          602.95 |        1454.67 |            9.77 |         9.88 |
| 5.0 MB      |            1166.15 |           2962.49 |          643.77 |         702.34 |         5001.88 |      5002.06 |

### Performance Comparison (Pisky vs Array Record)

**1.0 KB Records**:
- Write Speed: Pisky is 1.94x faster
- Read Speed: Pisky is 2.25x faster
- File Size: Pisky files are 1.01x smaller

**10.0 KB Records**:
- Write Speed: Pisky is 1.53x faster
- Read Speed: Pisky is 2.92x faster
- File Size: Pisky files are 1.01x smaller

**100.0 KB Records**:
- Write Speed: Pisky is 1.88x faster
- Read Speed: Pisky is 2.07x faster
- File Size: Pisky files are 1.01x smaller

**5.0 MB Records**:
- Write Speed: Pisky is 1.81x faster
- Read Speed: Pisky is 4.22x faster
- File Size: Pisky files are 1.00x smaller

## About the Benchmark

The benchmark tests with a range of record sizes:
- 1 KB (10,000 records, ~10 MB total)
- 10 KB (1,000 records, ~10 MB total)
- 100 KB (100 records, ~10 MB total)
- 5 MB (1,000 records, ~5 GB total)

Each test is run 3 times and the results are averaged.

## Setup

```bash
# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e .
```

## Running the Benchmark

```bash
python benchmark.py
```