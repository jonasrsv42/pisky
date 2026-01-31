# Pisky vs Array Record Performance Benchmark

This benchmark compares the performance of Pisky (v0.7.0) with Google's Array Record format for reading and writing data.

## Benchmark Results

### Summary
Pisky consistently outperforms Array Record across all tested record sizes:
- **Write speed**: 1.6-2.1x faster
- **Read speed**: 2.2-5.5x faster
- **File size**: Comparable (slightly smaller)

### Detailed Results

| Record Size | Pisky Write (MB/s) | Pisky Read (MB/s) | AR Write (MB/s) | AR Read (MB/s) | Pisky Size (MB) | AR Size (MB) |
|-------------|-------------------:|------------------:|----------------:|---------------:|----------------:|-------------:|
| 1.0 KB      |            1089.83 |           1570.76 |          569.77 |         663.54 |            9.79 |         9.88 |
| 10.0 KB     |            1032.18 |           2882.94 |          657.67 |        1301.62 |            9.77 |         9.88 |
| 100.0 KB    |            1128.46 |           3359.65 |          672.05 |        1448.15 |            9.77 |         9.88 |
| 5.0 MB      |            1492.14 |           4465.72 |          706.63 |         810.48 |         5001.88 |      5002.06 |

### Performance Comparison (Pisky vs Array Record)

**1.0 KB Records**:
- Write Speed: Pisky is 1.91x faster
- Read Speed: Pisky is 2.37x faster
- File Size: Pisky files are 1.01x smaller

**10.0 KB Records**:
- Write Speed: Pisky is 1.57x faster
- Read Speed: Pisky is 2.21x faster
- File Size: Pisky files are 1.01x smaller

**100.0 KB Records**:
- Write Speed: Pisky is 1.68x faster
- Read Speed: Pisky is 2.32x faster
- File Size: Pisky files are 1.01x smaller

**5.0 MB Records**:
- Write Speed: Pisky is 2.11x faster
- Read Speed: Pisky is 5.51x faster
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
