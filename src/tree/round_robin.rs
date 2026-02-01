//! Round-robin interleaving of multiple reader nodes.

use nanoserde::{DeJson, SerJson};
use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;

use disky::error::Result;
use disky::tree::reader::{Node, Reader, RoundRobinNode};

use super::node::PyNodeEnum;
use super::node::PyTreeReader;

/// Configuration for interleaving records from multiple sources in round-robin order.
///
/// Example (Python):
///     config = RoundRobinConfig([
///         RecordReaderConfig("a.disky"),
///         RecordReaderConfig("b.disky"),
///     ])
///     with config as reader:
///         for record in reader:
///             # Records arrive interleaved: a0, b0, a1, b1, ...
#[pyclass(name = "RoundRobinConfig")]
#[derive(Clone, SerJson, DeJson)]
pub struct PyRoundRobinConfig {
    pub children: Vec<PyNodeEnum>,
}

#[pymethods]
impl PyRoundRobinConfig {
    #[new]
    fn new(children: Vec<PyNodeEnum>) -> Self {
        Self { children }
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

impl PyRoundRobinConfig {
    fn build_internal(self) -> Result<Reader> {
        Box::new(self).make()
    }
}

impl Node for PyRoundRobinConfig {
    fn make(self: Box<Self>) -> Result<Reader> {
        let mut builder = RoundRobinNode::new();
        for child in self.children {
            builder = builder.append(child);
        }
        builder.build()
    }
}
