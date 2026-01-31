//! Reservoir shuffle for streaming data randomization.

use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;

use disky::error::Result;
use disky::tree::reader::{Node, Reader};
use disky::tree::sampling::ReservoirShuffleConfig;

use super::node::{PyNodeEnum, PyTreeReader};

/// Configuration for shuffling records from a source using reservoir sampling.
///
/// Example (Python):
///     config = ShuffleConfig(
///         RecordReaderConfig("data.disky"),
///         buffer_size=1000,
///         seed=42,
///     )
///     with config as reader:
///         for record in reader:
///             # Records arrive in shuffled order
#[pyclass(name = "ShuffleConfig")]
#[derive(Clone)]
pub struct PyShuffleConfig {
    child: Box<PyNodeEnum>,
    buffer_size: usize,
    seed: Option<u64>,
}

#[pymethods]
impl PyShuffleConfig {
    #[new]
    #[pyo3(signature = (child, buffer_size=1000, seed=None))]
    fn new(child: PyNodeEnum, buffer_size: usize, seed: Option<u64>) -> Self {
        Self {
            child: Box::new(child),
            buffer_size,
            seed,
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
}

impl PyShuffleConfig {
    fn build_internal(self) -> Result<Reader> {
        Box::new(self).make()
    }
}

impl Node for PyShuffleConfig {
    fn make(self: Box<Self>) -> Result<Reader> {
        let mut builder =
            ReservoirShuffleConfig::new(*self.child).with_buffer_size(self.buffer_size);

        if let Some(seed) = self.seed {
            builder = builder.with_seed(seed);
        }

        Ok(Box::new(builder.build()?))
    }
}
