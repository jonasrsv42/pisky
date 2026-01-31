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

- [x] Update imports to new module paths
- [x] Replace shard locator usage with new Shards + iterator pattern
- [x] Update writer to use `FileShardsBuilder`
- [x] Run tests to verify functionality

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

### Step 1: Fix compilation (Phase 1) ✅
Get pisky compiling with new disky APIs.

### Step 2: Core Rust configs ✅
Implement basic PyO3 config classes that map 1:1 to Rust nodes.

**Implemented:**
- `RecordReaderConfig` / `RecordWriterConfig` - Single file read/write
- `SequentialConfig` / `RoundRobinConfig` - Shard readers with Sequential/RandomRepeat order
- `SequentialWriterConfig` - Sharded sequential writer
- `MultiThreadedWriterConfig` / `MultiThreadedReaderConfig` - Parallel read/write
- `ShuffleConfig` - Reservoir shuffle
- `ThreadedConfig` - Offload to dedicated thread
- `SamplingConfig` - Weighted sampling from multiple sources
- `RoundRobinConfig` (tree) - Round-robin interleaving

### Step 3: Python wrappers ✅
Add Python wrapper classes with type hints and context manager support.

**Implemented in `pisky/tree/`:**
- `RoundRobinConfig` - Interleaving
- `ShuffleConfig` - Reservoir shuffle
- `ThreadedConfig` - Threaded offloading
- `SamplingConfig` - Weighted sampling

### Step 4: High-level abstractions 🚧
Add WeightedLeaf, WeightedSampling, graph transforms, and convenience functions.

### Step 5: Tests & docs ✅
Comprehensive tests for all composition patterns (88 tests passing).

---

---

## Phase 4: Weighted Tree Abstraction (Python-only)

### 4.1 Goal

Allow users to declare weights at **leaf nodes** and have a **smart parent node** automatically compute and propagate weights up the tree for sampling. This enables:

1. Intuitive weight specification at the data source level
2. Automatic rebalancing when tree structure changes
3. Nested weighted subtrees that compose naturally

### 4.2 Core Concepts

#### WeightedLeaf
A wrapper that attaches a weight to any `NodeConfig`. The weight represents the relative importance of this data source.

```python
from pisky.tree.weighted import WeightedLeaf

# Attach weight to a leaf node
english = WeightedLeaf(RecordReaderConfig("en.disky"), weight=2.0)
german = WeightedLeaf(RecordReaderConfig("de.disky"), weight=1.0)
```

**Key properties:**
- `weight: float` - The relative weight (must be positive)
- `child: NodeConfig` - The wrapped node config
- Implements `NodeConfig` protocol (can be used anywhere a node is expected)

#### WeightedSampling
A parent node that:
1. Walks its subtrees to find all `WeightedLeaf` nodes
2. Computes the **total weight** of each direct child subtree
3. Uses those computed weights for the underlying `SamplingConfig`

```python
from pisky.tree.weighted import WeightedSampling, WeightedLeaf

config = WeightedSampling([
    ThreadedConfig(WeightedLeaf(RecordReaderConfig("en.disky"), weight=2.0)),
    ThreadedConfig(WeightedLeaf(RecordReaderConfig("de.disky"), weight=1.0)),
])
# Subtree weights: [2.0, 1.0] -> SamplingConfig uses these
```

### 4.3 Weight Computation Rules

#### Rule 1: WeightedLeaf defines base weight
```
weight(WeightedLeaf(child, w)) = w
```

#### Rule 2: Chained WeightedLeaf is structurally impossible
```python
# WeightedLeaf.children = None (terminal in Python tree)
# WeightedLeaf constructor requires a NodeConfig with children property
# Since WeightedLeaf is terminal, it cannot be wrapped by another WeightedLeaf
WeightedLeaf(WeightedLeaf(x, 2.0), 3.0)  # Type error at construction
```
Rationale: Two-tree architecture makes this unrepresentable.

#### Rule 3: Non-weighted nodes have weight 1.0 by default
```
weight(RecordReaderConfig(...)) = 1.0
weight(ThreadedConfig(child)) = weight(child)
weight(ShuffleConfig(child)) = weight(child)
```

#### Rule 4: Multi-child nodes sum children weights
```
weight(RoundRobinConfig([a, b, c])) = weight(a) + weight(b) + weight(c)
```

#### Rule 5: SamplingConfig explicit weights act as multiplier
```
weight(SamplingConfig([(a, w1), (b, w2)])) = w1 * weight(a) + w2 * weight(b)
```

Example:
```python
SamplingConfig([
    (WeightedLeaf(x, 2.0), 3.0),  # contributes 2.0 * 3.0 = 6.0
    (WeightedLeaf(y, 1.0), 1.0),  # contributes 1.0 * 1.0 = 1.0
])
# Total weight of this subtree = 7.0
```

#### Rule 6: WeightedSampling computes weights (no multiplication)
```
WeightedSampling([a, b]) -> SamplingConfig([(a, weight(a)), (b, weight(b))])
```
Since it passes computed weight directly to SamplingConfig, no extra multiplication occurs.

#### Rule 7: WeightedSampling requires all leaves to be WeightedLeaf
```python
# ERROR: non-weighted leaf under WeightedSampling
WeightedSampling([
    RecordReaderConfig("a.disky"),  # ERROR: must be WeightedLeaf
    WeightedLeaf(RecordReaderConfig("b.disky"), 2.0),
])

# OK: all leaves are WeightedLeaf
WeightedSampling([
    WeightedLeaf(RecordReaderConfig("a.disky"), 1.0),
    WeightedLeaf(RecordReaderConfig("b.disky"), 2.0),
])
```
Rationale: Using WeightedSampling signals intent for explicit weight control. Implicit 1.0 defaults could hide bugs.

### 4.4 Usage Examples

#### Direct instantiation
```python
# Weight and child known upfront
config = WeightedSampling([
    WeightedLeaf(RecordReaderConfig("en.disky"), weight=2.0),
    WeightedLeaf(RecordReaderConfig("de.disky"), weight=1.0),
])
```

#### Lazy instantiation
```python
# Path and weight computed at runtime
def make_english():
    path = discover_latest_checkpoint("/data/en")
    weight = get_dataset_importance("en")
    return (RecordReaderConfig(path), weight)

def make_german():
    path = discover_latest_checkpoint("/data/de")
    weight = get_dataset_importance("de")
    return (RecordReaderConfig(path), weight)

# Factories called only when entering context manager
config = WeightedSampling([
    WeightedLeaf(make_english),
    WeightedLeaf(make_german),
])

# At this point, no paths resolved yet
# ...later...

with config as reader:  # NOW factories are called
    for record in reader:
        process(record)
```

#### Mixed direct and lazy
```python
config = WeightedSampling([
    # Static dataset - known upfront
    WeightedLeaf(RecordReaderConfig("base.disky"), weight=1.0),
    # Dynamic dataset - resolved at read time
    WeightedLeaf(lambda: (RecordReaderConfig(get_daily_path()), 0.5)),
])
```

### 4.5 Complex Example

```python
# Scenario: English data is 2x more important than German,
# German consists of news (3x) + books (1x)

config = WeightedSampling([
    # English subtree: total weight = 2.0
    ThreadedConfig(
        ShuffleConfig(
            WeightedLeaf(ShardReaderConfig("/data/en"), weight=2.0),
            buffer_size=1000,
        )
    ),
    # German subtree: total weight = 3.0 + 1.0 = 4.0
    ThreadedConfig(
        RoundRobinConfig([
            WeightedLeaf(ShardReaderConfig("/data/de/news"), weight=3.0),
            WeightedLeaf(ShardReaderConfig("/data/de/books"), weight=1.0),
        ])
    ),
])

# WeightedSampling computes:
#   Child 0 (English): weight = 2.0
#   Child 1 (German):  weight = 4.0
#
# Builds: SamplingConfig([
#   (ThreadedConfig(ShuffleConfig(ShardReaderConfig(...))), 2.0),
#   (ThreadedConfig(RoundRobinConfig([...])), 4.0),
# ])
```

### 4.5 Two-Tree Architecture

We maintain two separate tree views:

1. **Python tree** - for weight computation and validation (`children` property)
2. **Rust tree** - for actual execution (`_to_rust_node()` method)

Key insight: `WeightedLeaf` is **terminal in the Python tree** (no `children`), making chained weights structurally impossible. But it delegates to its wrapped node for the Rust tree.

```
Python Tree (weight computation)     Rust Tree (execution)
================================     =====================

WeightedSampling                     SamplingConfig
├── WeightedLeaf(w=2.0) [TERMINAL]       ├── ThreadedConfig
│                                        │   └── ShuffleConfig
│                                        │       └── RecordReader
└── WeightedLeaf(w=1.0) [TERMINAL]       └── RecordReader
```

### 4.6 Extended NodeConfig Protocol

Two separate tree traversals:
1. **Python tree** (`py_children`) - for weight computation, `WeightedLeaf` is terminal
2. **Rust tree** (`_to_rust_node()`) - for execution, `WeightedLeaf` is transparent

```python
# pisky/tree/node.py

@runtime_checkable
class NodeConfig(Protocol):
    """Protocol for configs that can be used as tree nodes."""

    def _to_rust_node(self) -> RustNode:
        """Build Rust tree for execution.

        WeightedLeaf delegates to its wrapped child (transparent).
        """
        ...

    @property
    def py_children(self) -> None | "NodeConfig" | Sequence["NodeConfig"]:
        """Return child node(s) for Python tree traversal.

        Used for weight computation and validation.
        WeightedLeaf returns None (terminal in Python tree).

        Returns:
            None - Leaf/terminal node (includes WeightedLeaf!)
            NodeConfig - Single child
            Sequence[NodeConfig] - Multiple children
        """
        ...

    @property
    def weight(self) -> float | None:
        """Return explicit weight, or None for default/computed.

        Returns:
            float - Explicit weight (WeightedLeaf)
            None - No explicit weight
        """
        ...
```

**Implementation by node type:**

| Node Type | `py_children` | `weight` | `_to_rust_node()` |
|-----------|---------------|----------|-------------------|
| `RecordReaderConfig` | `None` | `None` | Self |
| `ShardReaderConfig` | `None` | `None` | Self |
| `ShuffleConfig` | `self._child` | `None` | Wraps child's rust node |
| `ThreadedConfig` | `self._child` | `None` | Wraps child's rust node |
| `RoundRobinConfig` | `self._children` | `None` | Wraps children's rust nodes |
| `SamplingConfig` | `[c for c,w in self._sources]` | `None` | Wraps children's rust nodes |
| **`WeightedLeaf`** | **`None` (terminal!)** | `self._weight` | **Delegates to `_rust_child`** |
| `WeightedSampling` | `self._children` | `None` | Builds `SamplingConfig` |

**Example traversal:**
```python
# This tree:
ShuffleConfig(WeightedLeaf(RecordReaderConfig("a.disky"), 2.0))

# Python tree (via py_children) - for weight computation:
ShuffleConfig
└── WeightedLeaf [TERMINAL, weight=2.0]

# Rust tree (via _to_rust_node()) - for execution:
RustShuffleConfig
└── RustRecordReaderConfig
```

This makes chained `WeightedLeaf` structurally impossible - `WeightedLeaf.py_children = None` means Python tree traversal stops there, and weight computation sees it as a terminal node with an explicit weight.

### 4.6 Implementation

```python
# pisky/tree/weighted.py

from collections.abc import Sequence

def compute_subtree_weight(node: NodeConfig) -> float:
    """Recursively compute total weight of a subtree.

    Rules:
    1. If node has explicit weight, return it
    2. If node has no children (leaf), return 1.0
    3. If node has single child, return child's weight
    4. If node has multiple children, return sum of children weights
    """
    # Rule 1: Explicit weight
    if node.weight is not None:
        return node.weight

    children = node.children

    # Rule 2: Leaf node
    if children is None:
        return 1.0

    # Rule 3: Single child
    if isinstance(children, NodeConfig):
        return compute_subtree_weight(children)

    # Rule 4: Multiple children
    return sum(compute_subtree_weight(c) for c in children)


# Type for lazy WeightedLeaf factory
WeightedLeafFactory = Callable[[], tuple[NodeConfig, float]]


class WeightedLeaf:
    """Attaches a weight to any node config.

    IMPORTANT: WeightedLeaf is TERMINAL in the Python tree (children = None).
    This makes chained WeightedLeaf structurally impossible.

    Can be instantiated directly or lazily:

    Direct:
        WeightedLeaf(RecordReaderConfig("data.disky"), weight=2.0)

    Lazy (callable invoked on context manager entry):
        WeightedLeaf(lambda: (RecordReaderConfig(get_path()), 2.0))

    Lazy instantiation is useful when:
    - Path is computed at runtime
    - Config depends on external state
    - You want to defer file existence checks

    Note: Lazy leaves MUST be resolved before Rust tree building.
    WeightedSampling handles this automatically in __enter__.
    """

    @overload
    def __init__(self, child: NodeConfig, weight: float = 1.0) -> None: ...

    @overload
    def __init__(self, factory: WeightedLeafFactory) -> None: ...

    def __init__(
        self,
        child_or_factory: NodeConfig | WeightedLeafFactory,
        weight: float | None = None,
    ):
        if callable(child_or_factory) and weight is None:
            # Lazy mode: factory returns (child, weight)
            self._factory: WeightedLeafFactory | None = child_or_factory
            self._rust_child: NodeConfig | None = None  # For Rust tree only
            self._weight: float = 0.0  # Placeholder until resolved
        else:
            # Direct mode: child and weight provided
            if weight is None:
                weight = 1.0
            if weight <= 0:
                raise ValueError("weight must be positive")
            self._factory = None
            self._rust_child = child_or_factory  # type: ignore
            self._weight = weight

    def _resolve(self) -> None:
        """Resolve lazy factory. Called by WeightedSampling before building."""
        if self._factory is not None:
            child, weight = self._factory()
            if weight <= 0:
                raise ValueError("weight must be positive")
            self._rust_child = child
            self._weight = weight
            self._factory = None

    @property
    def children(self) -> None:
        """WeightedLeaf is TERMINAL in Python tree - no children."""
        return None

    @property
    def weight(self) -> float:
        return self._weight

    def _to_rust_node(self) -> RustNode:
        """Delegate to wrapped node for Rust tree."""
        assert self._rust_child is not None, "WeightedLeaf not resolved"
        return self._rust_child._to_rust_node()


def _resolve_lazy_leaves(node: NodeConfig) -> None:
    """Resolve all lazy WeightedLeaf nodes in Python tree."""
    if isinstance(node, WeightedLeaf):
        node._resolve()
        return  # WeightedLeaf is terminal in Python tree

    children = node.children
    if children is None:
        return
    if isinstance(children, NodeConfig):
        _resolve_lazy_leaves(children)
    else:
        for child in children:
            _resolve_lazy_leaves(child)


def _validate_all_leaves_weighted(node: NodeConfig) -> None:
    """Validate that all Python tree leaves are WeightedLeaf (Rule 7)."""
    if isinstance(node, WeightedLeaf):
        return  # WeightedLeaf is valid terminal

    children = node.children
    if children is None:
        # Non-WeightedLeaf terminal - error!
        raise ValueError(
            f"WeightedSampling requires all leaves to be WeightedLeaf, "
            f"found {type(node).__name__}"
        )

    if isinstance(children, NodeConfig):
        _validate_all_leaves_weighted(children)
    else:
        for child in children:
            _validate_all_leaves_weighted(child)


class WeightedSampling:
    """Samples from children using weights computed from subtrees."""

    def __init__(self, children: Sequence[NodeConfig], seed: int | None = None):
        self._children = list(children)
        self._seed = seed
        self._sampling: SamplingConfig | None = None

    @property
    def children(self) -> Sequence[NodeConfig]:
        return self._children

    @property
    def weight(self) -> float | None:
        return None  # Weight computed from children

    def __enter__(self) -> TreeReader:
        # Step 1: Resolve all lazy WeightedLeaf nodes (Python tree pass)
        for child in self._children:
            _resolve_lazy_leaves(child)

        # Step 2: Validate all Python tree leaves are WeightedLeaf (Rule 7)
        for child in self._children:
            _validate_all_leaves_weighted(child)

        # Step 3: Compute weight for each child subtree
        weighted_children = [
            (child, compute_subtree_weight(child))
            for child in self._children
        ]

        # Step 4: Build SamplingConfig and enter (Rust tree pass)
        self._sampling = SamplingConfig(weighted_children, seed=self._seed)
        return self._sampling.__enter__()

    def __exit__(self, *args):
        if self._sampling:
            return self._sampling.__exit__(*args)
        return False

    def _to_rust_node(self) -> RustNode:
        # For nested usage: resolve, validate, compute, delegate
        for child in self._children:
            _resolve_lazy_leaves(child)
            _validate_all_leaves_weighted(child)

        weighted_children = [
            (child, compute_subtree_weight(child))
            for child in self._children
        ]
        return SamplingConfig(weighted_children, seed=self._seed)._to_rust_node()
```

### 4.7 Tree Utilities

With the extended protocol, we can add useful tree utilities:

```python
# pisky/tree/utils.py

def walk_tree(node: NodeConfig) -> Iterator[NodeConfig]:
    """Depth-first traversal of all nodes in tree."""
    yield node
    children = node.children
    if children is None:
        return
    if isinstance(children, NodeConfig):
        yield from walk_tree(children)
    else:
        for child in children:
            yield from walk_tree(child)


def find_weighted_leaves(node: NodeConfig) -> list[WeightedLeaf]:
    """Find all WeightedLeaf nodes in tree."""
    return [n for n in walk_tree(node) if isinstance(n, WeightedLeaf)]


def print_tree(node: NodeConfig, indent: int = 0) -> None:
    """Print tree structure for debugging."""
    prefix = "  " * indent
    weight_str = f" (weight={node.weight})" if node.weight is not None else ""
    print(f"{prefix}{type(node).__name__}{weight_str}")

    children = node.children
    if children is None:
        return
    if isinstance(children, NodeConfig):
        print_tree(children, indent + 1)
    else:
        for child in children:
            print_tree(child, indent + 1)
```

### 4.8 Design Decisions

1. **Protocol-based tree traversal** - All nodes implement `children` and `weight` properties, enabling uniform tree operations.

2. **WeightedLeaf is transparent** - When building the Rust tree, it just delegates to its child. The weight is only used during weight computation.

3. **Default weight is 1.0** - Leaf nodes without explicit weight contribute 1.0 so they're not ignored.

4. **Weights are relative** - Only ratios matter. `[2.0, 1.0]` == `[0.67, 0.33]` == `[20, 10]`.

5. **WeightedSampling computes per direct child** - Each direct child's subtree weight is summed, then used for sampling.

6. **No normalization** - Raw computed weights passed to SamplingConfig. Rust sampler normalizes internally.

### 4.10 Open Questions

1. ~~**Should WeightedLeaf support nested WeightedLeafs?**~~
   - **Resolved**: Structurally impossible. WeightedLeaf is terminal in Python tree (`children = None`), so it cannot be wrapped by another WeightedLeaf.

2. **How to handle WeightedLeaf outside WeightedSampling?**
   - Current: Works fine, weight is ignored (transparent wrapper)
   - Alternative: Warn that weight has no effect
   - Recommendation: Silent (weight may be used by future abstractions)

3. **Should SamplingConfig's explicit weights multiply with subtree weights?**
   ```python
   SamplingConfig([
       (WeightedLeaf(a, 2.0), 3.0),  # 2.0 * 3.0 = 6.0 ?
       (WeightedLeaf(b, 1.0), 1.0),  # 1.0 * 1.0 = 1.0 ?
   ])
   ```
   - Current: Explicit weight (3.0, 1.0) used, subtree weight ignored
   - Alternative: Multiply explicit * subtree
   - Recommendation: Explicit wins (SamplingConfig already has weights)

4. **Should nodes be frozen after construction?**
   - Current: Mutable (can modify `_child`, `_children`)
   - Alternative: Frozen dataclass style
   - Recommendation: Keep mutable for simplicity, document as "configure before use"

5. **How to handle tree traversal with unresolved lazy leaves?**
   - Simple: Force resolve all lazy leaves in `__enter__` before any traversal
   - `_resolve_tree()` walks tree and calls `_resolve()` on all `WeightedLeaf` nodes
   - After resolution, `children` and `weight` work normally (assert if not resolved)
   - No need for `None` handling in traversal code

---

## Notes

- All tree nodes require `Send` in Rust (see `disky::tree::reader::Node`)
- Python builds the tree structure; Rust does all the actual iteration
- Context manager ensures proper cleanup (thread joins, file closes)
- GIL is released during iteration for better parallelism
