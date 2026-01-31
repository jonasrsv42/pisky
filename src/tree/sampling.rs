//! Sampling reader for weighted random sampling from multiple sources.

use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;

use disky::error::Result;
use disky::tree::reader::{Node, Reader};
use disky::tree::sampling::SamplingReaderConfig;

use super::node::{PyNodeEnum, PyTreeReader};

/// Configuration for sampling from multiple weighted sources.
///
/// Samples from multiple child nodes based on their weights. Higher weights
/// mean the source is sampled more frequently.
///
/// Example (Python):
///     config = SamplingConfig(
///         [
///             (RecordReaderConfig("a.disky"), 2.0),
///             (RecordReaderConfig("b.disky"), 1.0),
///         ],
///         seed=42,
///     )
///     with config as reader:
///         for record in reader:
///             # Records sampled proportionally from sources
#[pyclass(name = "SamplingConfig")]
#[derive(Clone)]
pub struct PySamplingConfig {
    sources: Vec<(Box<PyNodeEnum>, f64)>,
    seed: Option<u64>,
}

#[pymethods]
impl PySamplingConfig {
    #[new]
    #[pyo3(signature = (sources, seed=None))]
    fn new(sources: Vec<(PyNodeEnum, f64)>, seed: Option<u64>) -> PyResult<Self> {
        if sources.is_empty() {
            return Err(PyValueError::new_err("sources cannot be empty"));
        }
        if sources.iter().any(|(_, w)| *w <= 0.0) {
            return Err(PyValueError::new_err("all weights must be positive"));
        }

        Ok(Self {
            sources: sources
                .into_iter()
                .map(|(node, weight)| (Box::new(node), weight))
                .collect(),
            seed,
        })
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

impl PySamplingConfig {
    fn build_internal(self) -> Result<Reader> {
        Box::new(self).make()
    }
}

impl Node for PySamplingConfig {
    fn make(self: Box<Self>) -> Result<Reader> {
        let mut builder = SamplingReaderConfig::new();

        for (child, weight) in self.sources.into_iter() {
            builder = builder.append(weight, *child);
        }

        if let Some(seed) = self.seed {
            builder = builder.with_seed(seed);
        }

        Ok(Box::new(builder.build()?))
    }
}
