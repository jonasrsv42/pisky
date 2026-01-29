# Pisky Update Plan

Update Python bindings to support new Disky APIs and expose a composable, config-based reader/writer architecture.

---

## Phase 1: Fix Broken Code (Old APIs)

The Rust codebase has undergone significant refactoring.

### 1.1 API Migration Map

| Old API | New API |
|---------|---------|
| `FileShardLocator` | `FileShards` + `SequentialShardSource` |
| `MultiPathShardLocator` | `FileShards::new(paths)` + `SequentialShardSource` |
| `RandomMultiPathShardLocator` | `FileShards::new(paths)` + `RandomRepeatingShardSource` |
| `FileSharder` / `FileSharderConfig` | `sink::FileShardsBuilder` |
| `ShardLocator` trait | `Shards` trait + iterator wrappers |
| `ParallelReaderConfig.reader_config` | `ParallelReaderConfig.reader_options` |

### 1.2 Files to Update

- `src/multi_threaded.rs` - Reader/Writer factory methods
- `src/shard_helpers.rs` - `create_multi_threaded_reader()` helper

### 1.3 Action Items

- [ ] Update imports to new module paths
- [ ] Replace shard locator usage with new Shards + iterator pattern
- [ ] Update writer to use `FileShardsBuilder`
- [ ] Run tests to verify functionality

---

## Phase 2: Python API Design

### 2.1 Design Principles

1. **Config objects** - Describe what you want, don't execute until needed
2. **Context manager materializes** - Enter `with config as reader:` to build
3. **Composable** - Configs nest arbitrarily to form trees
4. **Uniform** - Same pattern for simple and complex cases
5. **Preprocessing in Python** - Graph transformations before Rust execution

### 2.2 Core Config Types

```python
# Leaf nodes (data sources)
RecordReaderConfig(path)                    # Single file
ShardReaderConfig(directory, prefix)        # Sequential shards
RepeatingShardReaderConfig(directory, prefix)  # Infinite, reshuffling

# Transform nodes (single child)
ShuffleConfig(child, buffer_size=42)        # Reservoir shuffle
ThreadedConfig(child, buffer_size=64)       # Offload to dedicated thread

# Aggregation nodes (multiple children)
SamplingConfig().add(weight, child)...      # Weighted sampling
InterleavingConfig().add(child)...          # Round-robin interleave

# Writers (don't compose, but same ctx manager semantics)
RecordWriterConfig(path)                    # Single file
ShardWriterConfig(directory, prefix, max_bytes=None)  # Rotating shards
ParallelWriterConfig(directory, prefix, workers=4, ...)  # Multi-threaded
```

### 2.3 Usage Examples

#### Simple: Single file
```python
from pisky import RecordReaderConfig, RecordWriterConfig

# Read
with RecordReaderConfig("data.disky") as reader:
    for record in reader:
        process(record)

# Write
with RecordWriterConfig("data.disky") as writer:
    writer.write(b"hello")
```

#### Shards: Sequential reading
```python
from pisky import ShardReaderConfig

with ShardReaderConfig("/data/train", prefix="shard") as reader:
    for record in reader:
        process(record)
```

#### Parallel: Multi-threaded with work stealing
```python
from pisky import ParallelReaderConfig, ShardReaderConfig

config = ParallelReaderConfig(
    ShardReaderConfig("/data/train", prefix="shard"),
    workers=4
)

with config as reader:
    for record in reader:
        process(record)
```

#### Tree: Complex composition
```python
from pisky import (
    ShardReaderConfig,
    SamplingConfig,
    ShuffleConfig,
    ThreadedConfig,
)

# Each source on its own thread, weighted sampling, shuffled
config = ShuffleConfig(
    SamplingConfig()
        .add(1.0, ThreadedConfig(ShardReaderConfig("/data/en", "shard")))
        .add(0.5, ThreadedConfig(ShardReaderConfig("/data/de", "shard")))
        .add(0.5, ThreadedConfig(ShardReaderConfig("/data/fr", "shard"))),
    buffer_size=10000
)

with config as reader:
    for record in reader:
        train(record)
```

#### ML Training: Infinite repeating with shuffle
```python
from pisky import RepeatingShardReaderConfig, ShuffleConfig

config = ShuffleConfig(
    RepeatingShardReaderConfig("/data/train", prefix="shard"),
    buffer_size=10000
)

with config as reader:
    for record in reader:  # Never ends, reshuffles each epoch
        train(record)
```

#### Writers: Same context manager pattern
```python
from pisky import RecordWriterConfig, ShardWriterConfig, ParallelWriterConfig

# Single file
with RecordWriterConfig("output.disky", compression="zstd") as writer:
    writer.write(b"record1")
    writer.write(b"record2")

# Rotating shards (new file every max_bytes)
with ShardWriterConfig("/data/out", prefix="shard", max_bytes=1_000_000_000) as writer:
    for record in dataset:
        writer.write(record)

# Multi-threaded parallel writing
with ParallelWriterConfig(
    "/data/out",
    prefix="shard",
    workers=4,
    shards=8,
    compression="zstd"
) as writer:
    for record in dataset:
        writer.write(record)  # Distributed across workers
```

### 2.4 Higher-Level Abstractions (Python-side graph transforms)

Beyond 1:1 Rust mappings, we can add Python-only abstractions that preprocess
the config graph before building the Rust tree.

#### WeightedNode - Associate weight with any reader
```python
from pisky import WeightedNode, ShardReaderConfig

# Attach weight to a node (doesn't change behavior alone)
weighted_en = WeightedNode(1.0, ShardReaderConfig("/data/en", "shard"))
weighted_de = WeightedNode(0.5, ShardReaderConfig("/data/de", "shard"))
```

#### WeightedSamplingNode - Auto-collect weighted children
```python
from pisky import WeightedSamplingNode, WeightedNode, ThreadedConfig

# Automatically finds all WeightedNode descendants and builds SamplingConfig
config = WeightedSamplingNode(
    ThreadedConfig(WeightedNode(1.0, ShardReaderConfig("/data/en", "shard"))),
    ThreadedConfig(WeightedNode(0.5, ShardReaderConfig("/data/de", "shard"))),
    ThreadedConfig(WeightedNode(0.5, ShardReaderConfig("/data/fr", "shard"))),
)

# During build(), this walks the tree, extracts weights, and creates:
# SamplingConfig()
#     .add(1.0, ThreadedConfig(ShardReaderConfig("/data/en", "shard")))
#     .add(0.5, ThreadedConfig(ShardReaderConfig("/data/de", "shard")))
#     .add(0.5, ThreadedConfig(ShardReaderConfig("/data/fr", "shard")))
```

#### Graph preprocessing possibilities

1. **Weight normalization** - Auto-normalize weights to sum to 1.0
2. **Auto-threading** - Wrap all leaf nodes in ThreadedConfig
3. **Validation** - Check for cycles, missing files, invalid configs
4. **Optimization** - Merge adjacent shuffles, remove redundant nodes
5. **Visualization** - Print tree structure for debugging

```python
# Example: auto-thread all leaves
config = auto_thread(
    SamplingConfig()
        .add(1.0, ShardReaderConfig("/data/en", "shard"))
        .add(0.5, ShardReaderConfig("/data/de", "shard"))
)
# Transforms to:
# SamplingConfig()
#     .add(1.0, ThreadedConfig(ShardReaderConfig("/data/en", "shard")))
#     .add(0.5, ThreadedConfig(ShardReaderConfig("/data/de", "shard")))
```

### 2.5 Implementation Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Python Layer                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  High-level abstractions (WeightedNode, auto_thread) │   │
│  │  Graph transforms, validation, visualization         │   │
│  └─────────────────────────────────────────────────────┘   │
│                           │                                  │
│                           ▼                                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Core Config classes (ShardReaderConfig, etc.)       │   │
│  │  Thin wrappers around Rust Py* classes               │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                            │
                            │ __enter__() calls build()
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                      Rust Layer                              │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  PyO3 Config classes (PyShardReaderConfig, etc.)     │   │
│  │  Store config, implement build() -> iterator         │   │
│  └─────────────────────────────────────────────────────┘   │
│                           │                                  │
│                           ▼                                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Disky tree nodes (SequentialShardReaderConfig, etc.)│   │
│  │  Actual reading happens here                         │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### 2.6 Rust PyO3 Classes Needed

```rust
// src/tree.rs - Reader configs

#[pyclass]
struct PyRecordReaderConfig { path: PathBuf, ... }

#[pyclass]
struct PyShardReaderConfig { directory: PathBuf, prefix: String, ... }

#[pyclass]
struct PyRepeatingShardReaderConfig { directory: PathBuf, prefix: String, ... }

#[pyclass]
struct PyShuffleConfig { child: PyObject, buffer_size: usize, seed: Option<u64> }

#[pyclass]
struct PyThreadedConfig { child: PyObject, buffer_size: usize }

#[pyclass]
struct PySamplingConfig { children: Vec<(f64, PyObject)> }

#[pyclass]
struct PyInterleavingConfig { children: Vec<PyObject> }

// src/writers.rs - Writer configs

#[pyclass]
struct PyRecordWriterConfig { path: PathBuf, compression: Option<String>, ... }

#[pyclass]
struct PyShardWriterConfig { directory: PathBuf, prefix: String, max_bytes: Option<usize>, ... }

#[pyclass]
struct PyParallelWriterConfig {
    directory: PathBuf,
    prefix: String,
    workers: usize,
    shards: usize,
    compression: Option<String>,
    ...
}

// Common traits
trait PyReaderConfig {
    fn build(&self) -> PyResult<Box<dyn Iterator<Item = PyResult<PyBytes>>>>;
}

trait PyWriterConfig {
    fn build(&self) -> PyResult<PyWriter>;  // PyWriter wraps actual writer
}
```

### 2.7 Open Questions

1. **How to pass child configs to Rust?**
   - Option A: PyObject and downcast in Rust
   - Option B: Enum of all config types
   - Option C: Build tree in Python, only call Rust for leaf nodes

2. **Where does graph transformation happen?**
   - Pure Python (recommended) - more flexible, easier to debug
   - Rust - more efficient but harder to extend

3. **Error handling for invalid trees?**
   - Validate on construction (fail early)
   - Validate on build (fail when entering context)
   - Both (warn on construction, error on build)

4. **Should configs be mutable or immutable?**
   - Mutable: `config.add(...)` modifies in place
   - Immutable: `config.add(...)` returns new config
   - Recommendation: Immutable (functional style, no surprises)

---

## Phase 3: Implementation Steps

### Step 1: Fix compilation (Phase 1)
Get pisky compiling with new disky APIs.

### Step 2: Core Rust configs
Implement basic PyO3 config classes that map 1:1 to Rust nodes.

### Step 3: Python wrappers
Add Python wrapper classes with type hints and context manager support.

### Step 4: High-level abstractions
Add WeightedNode, graph transforms, and convenience functions.

### Step 5: Tests & docs
Comprehensive tests for all composition patterns.

---

## Notes

- All tree nodes require `Send` in Rust (see `disky::tree::reader::Node`)
- Python builds the tree structure; Rust does all the actual iteration
- Context manager ensures proper cleanup (thread joins, file closes)
- GIL is released during iteration for better parallelism
