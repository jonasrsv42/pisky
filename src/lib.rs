use log::LevelFilter;
use pyo3::prelude::*;

// New config-based API modules
mod compression;
mod multi_threaded;
pub mod shard;
mod single;
pub mod tree;

// Shared utilities
mod corruption;
mod logging;

// New API types
use compression::{PyUncompressed, PyZstd};
use multi_threaded::{
    PyMultiThreadedReader, PyMultiThreadedReaderConfig, PyMultiThreadedWriterConfig,
    PyMultiThreadedWriterInstance,
};
use shard::{
    PyRandomRepeatOrder, PyReaderFileShards, PyRoundRobinReaderConfig, PySequentialOrder,
    PySequentialReaderConfig, PySequentialWriter, PySequentialWriterConfig, PyShardReader,
    PyWriterFileShards,
};
use single::{PyRecordReader, PyRecordReaderConfig, PyRecordWriter, PyRecordWriterConfig};
use tree::{
    PyRoundRobinConfig, PySamplingConfig, PyShuffleConfig, PyThreadedConfig, PyTreeReader,
    tree_from_bytes,
};

// Legacy types
use corruption::PyCorruptionStrategy;
use logging::{init_logger, set_log_level};

/// Python module for low-level Disky bindings
#[pymodule]
fn _pisky(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialize logger with info level by default
    init_logger(LevelFilter::Info);

    // New config-based API - single file
    m.add_class::<PyRecordWriterConfig>()?;
    m.add_class::<PyRecordReaderConfig>()?;
    m.add_class::<PyRecordWriter>()?;
    m.add_class::<PyRecordReader>()?;
    m.add_class::<PyZstd>()?;
    m.add_class::<PyUncompressed>()?;

    // New config-based API - sharded readers (2 configs + 2 orders + 1 shared reader)
    m.add_class::<PyReaderFileShards>()?;
    m.add_class::<PySequentialOrder>()?;
    m.add_class::<PyRandomRepeatOrder>()?;
    m.add_class::<PySequentialReaderConfig>()?;
    m.add_class::<PyRoundRobinReaderConfig>()?;
    m.add_class::<PyShardReader>()?;

    // New config-based API - sharded writers
    m.add_class::<PyWriterFileShards>()?;
    m.add_class::<PySequentialWriterConfig>()?;
    m.add_class::<PySequentialWriter>()?;

    // New config-based API - multi-threaded reader (1 config + 1 shared reader)
    m.add_class::<PyMultiThreadedReaderConfig>()?;
    m.add_class::<PyMultiThreadedReader>()?;

    // New config-based API - multi-threaded writers (uses PyWriterFileShards from sharded writers)
    m.add_class::<PyMultiThreadedWriterConfig>()?;
    m.add_class::<PyMultiThreadedWriterInstance>()?;

    // Legacy classes
    m.add_class::<PyCorruptionStrategy>()?;

    // Tree-based composition API
    m.add_class::<PyRoundRobinConfig>()?;
    m.add_class::<PySamplingConfig>()?;
    m.add_class::<PyShuffleConfig>()?;
    m.add_class::<PyThreadedConfig>()?;
    m.add_class::<PyTreeReader>()?;

    // Add functions to the module
    m.add_function(wrap_pyfunction!(set_log_level, m)?)?;
    m.add_function(wrap_pyfunction!(tree_from_bytes, m)?)?;

    Ok(())
}
