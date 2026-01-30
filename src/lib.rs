use log::LevelFilter;
use pyo3::prelude::*;

// New config-based API modules
mod compression;
mod multi_threaded;
mod shard;
mod single;
mod tree;

// Shared utilities
mod corruption;
mod logging;

// New API types
use compression::{PyUncompressed, PyZstd};
use multi_threaded::{
    PyMultiThreadedReader, PyMultiThreadedReaderRandOrderConfig,
    PyMultiThreadedReaderSeqOrderConfig, PyMultiThreadedWriterConfig,
    PyMultiThreadedWriterInstance,
};
use shard::{
    PyRRReaderRandOrderConfig, PyRRReaderSeqOrderConfig, PyReaderFileShards,
    PySeqReaderRandOrderConfig, PySeqReaderSeqOrderConfig, PySequentialWriter,
    PySequentialWriterConfig, PyShardReader, PyWriterFileShards,
};
use single::{PyRecordReader, PyRecordReaderConfig, PyRecordWriter, PyRecordWriterConfig};
use tree::{PyRoundRobinConfig, PyTreeReader};

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

    // New config-based API - sharded readers (4 configs + 1 shared reader)
    m.add_class::<PyReaderFileShards>()?;
    m.add_class::<PySeqReaderSeqOrderConfig>()?;
    m.add_class::<PySeqReaderRandOrderConfig>()?;
    m.add_class::<PyRRReaderSeqOrderConfig>()?;
    m.add_class::<PyRRReaderRandOrderConfig>()?;
    m.add_class::<PyShardReader>()?;

    // New config-based API - sharded writers
    m.add_class::<PyWriterFileShards>()?;
    m.add_class::<PySequentialWriterConfig>()?;
    m.add_class::<PySequentialWriter>()?;

    // New config-based API - multi-threaded readers (2 configs + 1 shared reader)
    m.add_class::<PyMultiThreadedReaderSeqOrderConfig>()?;
    m.add_class::<PyMultiThreadedReaderRandOrderConfig>()?;
    m.add_class::<PyMultiThreadedReader>()?;

    // New config-based API - multi-threaded writers (uses PyWriterFileShards from sharded writers)
    m.add_class::<PyMultiThreadedWriterConfig>()?;
    m.add_class::<PyMultiThreadedWriterInstance>()?;

    // Legacy classes
    m.add_class::<PyCorruptionStrategy>()?;

    // Tree-based composition API
    m.add_class::<PyRoundRobinConfig>()?;
    m.add_class::<PyTreeReader>()?;

    // Add functions to the module
    m.add_function(wrap_pyfunction!(set_log_level, m)?)?;

    Ok(())
}
