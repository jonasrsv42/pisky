//! Shard reader PyO3 bindings.
//!
//! Provides 2 reader configs that accept order objects:
//! - SequentialReaderConfig: drains each shard before moving to the next
//! - RoundRobinReaderConfig: interleaves records across shards

use nanoserde::{DeJson, SerJson};
use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;

use disky::shard::reader::{RoundRobinShardReaderConfig, SequentialShardReaderConfig};
use disky::tree::reader::{Node, Reader};

use crate::tree::node::PyNodeEnum;

use super::order::ShardOrder;

// =============================================================================
// Shared reader wrapper
// =============================================================================

/// Active shard reader - iterates over records from shards.
///
/// This is the shared reader type returned by all shard reader configs.
#[pyclass(name = "ShardReader")]
pub struct PyShardReader {
    reader: Option<Reader>,
}

impl PyShardReader {
    pub fn new(reader: Reader) -> Self {
        Self {
            reader: Some(reader),
        }
    }
}

#[pymethods]
impl PyShardReader {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<Option<pyo3_bytes::PyBytes>> {
        let reader = self
            .reader
            .as_mut()
            .ok_or_else(|| PyIOError::new_err("Reader is closed"))?;

        match reader.next() {
            Some(Ok(bytes)) => Ok(Some(pyo3_bytes::PyBytes::new(bytes))),
            Some(Err(e)) => Err(PyIOError::new_err(e.to_string())),
            None => Ok(None),
        }
    }

    /// Close the reader, releasing all underlying file handles.
    fn close(&mut self) {
        self.reader.take();
    }
}

// =============================================================================
// Sequential reader config
// =============================================================================

/// Sequential shard reader - drains each shard completely before moving to the next.
///
/// Example:
///     shards = FileShards.from_pattern("/data", "shard")
///     order = SequentialOrder(shards)
///     with SequentialReaderConfig(order) as reader:
///         for record in reader:
///             process(record)
#[pyclass(name = "SequentialReaderConfig", from_py_object)]
#[derive(Clone, SerJson, DeJson)]
pub struct PySequentialReaderConfig {
    pub order: ShardOrder,
}

impl PySequentialReaderConfig {
    fn build_reader(&self) -> disky::error::Result<Reader> {
        let source = self.order.clone().into_source()?;
        let reader = SequentialShardReaderConfig::new(source).build();
        Ok(Box::new(reader))
    }
}

#[pymethods]
impl PySequentialReaderConfig {
    #[new]
    fn new(order: ShardOrder) -> Self {
        Self { order }
    }

    fn __enter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyShardReader>> {
        let config = slf.borrow(py);
        let reader = config
            .build_reader()
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        Py::new(py, PyShardReader::new(reader))
    }

    fn __exit__(
        &self,
        _a: Option<Bound<'_, PyAny>>,
        _b: Option<Bound<'_, PyAny>>,
        _c: Option<Bound<'_, PyAny>>,
    ) -> bool {
        false
    }

    /// Serialize this config to bytes for cross-library transfer.
    fn serialize_as_bytes(&self) -> Vec<u8> {
        PyNodeEnum::from(self.clone()).serialize_json().into_bytes()
    }
}

impl Node for PySequentialReaderConfig {
    fn make(self: Box<Self>) -> disky::error::Result<Reader> {
        self.build_reader()
    }
}

// =============================================================================
// Round-robin reader config
// =============================================================================

/// Round-robin shard reader - reads one record from each shard in rotation.
///
/// Example:
///     shards = FileShards.from_pattern("/data", "shard")
///     order = SequentialOrder(shards)
///     with RoundRobinReaderConfig(order, max_active=4) as reader:
///         for record in reader:
///             process(record)
#[pyclass(name = "RoundRobinReaderConfig", from_py_object)]
#[derive(Clone, SerJson, DeJson)]
pub struct PyRoundRobinReaderConfig {
    pub order: ShardOrder,
    pub max_active: Option<usize>,
}

impl PyRoundRobinReaderConfig {
    fn build_reader(&self) -> disky::error::Result<Reader> {
        let source = self.order.clone().into_source()?;
        let mut builder = RoundRobinShardReaderConfig::new(source);
        if let Some(max) = self.max_active {
            builder = builder.max_active(max);
        }
        Ok(Box::new(builder.build()?))
    }
}

#[pymethods]
impl PyRoundRobinReaderConfig {
    #[new]
    #[pyo3(signature = (order, max_active=None))]
    fn new(order: ShardOrder, max_active: Option<usize>) -> Self {
        Self { order, max_active }
    }

    fn __enter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyShardReader>> {
        let config = slf.borrow(py);
        let reader = config
            .build_reader()
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        Py::new(py, PyShardReader::new(reader))
    }

    fn __exit__(
        &self,
        _a: Option<Bound<'_, PyAny>>,
        _b: Option<Bound<'_, PyAny>>,
        _c: Option<Bound<'_, PyAny>>,
    ) -> bool {
        false
    }

    /// Serialize this config to bytes for cross-library transfer.
    fn serialize_as_bytes(&self) -> Vec<u8> {
        PyNodeEnum::from(self.clone()).serialize_json().into_bytes()
    }
}

impl Node for PyRoundRobinReaderConfig {
    fn make(self: Box<Self>) -> disky::error::Result<Reader> {
        self.build_reader()
    }
}
