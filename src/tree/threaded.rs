//! Threaded node for offloading subtrees to dedicated threads.

use nanoserde::{DeJson, SerJson};
use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;

use disky::error::Result;
use disky::parallel::node::ThreadedNodeConfig;
use disky::tree::reader::{Node, Reader};

use super::node::PyNodeEnum;
use super::node::PyTreeReader;

/// Configuration for running a child node on a dedicated thread.
///
/// This wraps a child node and runs it on a dedicated thread, communicating
/// results back via a bounded channel. Useful for parallelizing I/O-bound
/// subtrees or decoupling producer from consumer.
///
/// Example (Python):
///     config = ThreadedConfig(
///         RecordReaderConfig("data.disky"),
///         buffer_size=128,
///     )
///     with config as reader:
///         for record in reader:
///             # Records are prefetched on a separate thread
#[pyclass(name = "ThreadedConfig")]
#[derive(Clone, SerJson, DeJson)]
pub struct PyThreadedConfig {
    pub child: Box<PyNodeEnum>,
    pub buffer_size: usize,
}

#[pymethods]
impl PyThreadedConfig {
    #[new]
    #[pyo3(signature = (child, buffer_size=64))]
    fn new(child: PyNodeEnum, buffer_size: usize) -> Self {
        Self {
            child: Box::new(child),
            buffer_size,
        }
    }

    /// Build the tree and return an iterator.
    fn build(&self) -> PyResult<PyTreeReader> {
        let reader = self
            .clone()
            .build_internal()
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        Ok(PyTreeReader::new(reader))
    }

    fn __enter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyTreeReader>> {
        let config = slf.borrow(py);
        let reader = config
            .clone()
            .build_internal()
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        Py::new(py, PyTreeReader::new(reader))
    }

    fn __exit__(
        &self,
        _exc_type: Option<Bound<'_, PyAny>>,
        _exc_value: Option<Bound<'_, PyAny>>,
        _traceback: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        Ok(false)
    }

    /// Serialize this config to bytes for cross-library transfer.
    fn serialize_as_bytes(&self) -> Vec<u8> {
        PyNodeEnum::from(self.clone()).serialize_json().into_bytes()
    }
}

impl PyThreadedConfig {
    fn build_internal(self) -> Result<Reader> {
        Box::new(self).make()
    }
}

impl Node for PyThreadedConfig {
    fn make(self: Box<Self>) -> Result<Reader> {
        let builder = ThreadedNodeConfig::new(*self.child).with_buffer_size(self.buffer_size);

        Ok(Box::new(builder.build()?))
    }
}
