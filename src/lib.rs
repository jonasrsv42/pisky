use log::LevelFilter;
use pyo3::prelude::*;

// Define modules
mod corruption;
mod single_threaded;
mod multi_threaded;
mod shard_helpers;
mod logging;

// Import types and functions from modules
use corruption::PyCorruptionStrategy;
use single_threaded::{PyRecordReader, PyRecordWriter};
use multi_threaded::{PyMultiThreadedReader, PyMultiThreadedWriter};
use logging::{init_logger, set_log_level};

/// Python module for low-level Disky bindings
#[pymodule]
fn _pisky(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialize logger with info level by default
    init_logger(LevelFilter::Info);
    
    // Add classes to the module
    m.add_class::<PyRecordWriter>()?;
    m.add_class::<PyRecordReader>()?;
    m.add_class::<PyMultiThreadedWriter>()?;
    m.add_class::<PyMultiThreadedReader>()?;
    m.add_class::<PyCorruptionStrategy>()?;

    // Add functions to the module
    m.add_function(wrap_pyfunction!(set_log_level, m)?)?;

    Ok(())
}