use log::LevelFilter;
use pyo3::prelude::*;

// New config-based API modules
mod compression;
mod shard;
mod single;

// Shared utilities
mod corruption;
mod logging;
mod multi_threaded;
mod shard_helpers;

// New API types
use compression::{PyUncompressed, PyZstd};
use shard::{
    PyRRReaderRandOrder, PyRRReaderRandOrderReader, PyRRReaderSeqOrder, PyRRReaderSeqOrderReader,
    PyReaderFileShards, PySeqReaderRandOrder, PySeqReaderRandOrderReader, PySeqReaderSeqOrder,
    PySeqReaderSeqOrderReader, PySequentialWriter, PySequentialWriterConfig, PyWriterFileShards,
};
use single::{PyRecordReader, PyRecordReaderConfig, PyRecordWriter, PyRecordWriterConfig};

// Legacy types
use corruption::PyCorruptionStrategy;
use logging::{init_logger, set_log_level};
use multi_threaded::{PyMultiThreadedReader, PyMultiThreadedWriter};

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

    // New config-based API - sharded readers (4 combinations)
    m.add_class::<PyReaderFileShards>()?;
    m.add_class::<PySeqReaderSeqOrder>()?;
    m.add_class::<PySeqReaderSeqOrderReader>()?;
    m.add_class::<PySeqReaderRandOrder>()?;
    m.add_class::<PySeqReaderRandOrderReader>()?;
    m.add_class::<PyRRReaderSeqOrder>()?;
    m.add_class::<PyRRReaderSeqOrderReader>()?;
    m.add_class::<PyRRReaderRandOrder>()?;
    m.add_class::<PyRRReaderRandOrderReader>()?;

    // New config-based API - sharded writers
    m.add_class::<PyWriterFileShards>()?;
    m.add_class::<PySequentialWriterConfig>()?;
    m.add_class::<PySequentialWriter>()?;

    // Legacy classes (will be removed)
    m.add_class::<PyMultiThreadedWriter>()?;
    m.add_class::<PyMultiThreadedReader>()?;
    m.add_class::<PyCorruptionStrategy>()?;

    // Add functions to the module
    m.add_function(wrap_pyfunction!(set_log_level, m)?)?;

    Ok(())
}
