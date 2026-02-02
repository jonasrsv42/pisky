//! Node enum and tree reader for Python bindings.

use nanoserde::{DeJson, SerJson};
use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;

use disky::error::Result;
use disky::tree::reader::{Node, Reader};

use crate::multi_threaded::PyMultiThreadedReaderConfig;
use crate::shard::{PyRoundRobinReaderConfig, PySequentialReaderConfig};
use crate::single::PyRecordReaderConfig;

use super::{PyRoundRobinConfig, PySamplingConfig, PyShuffleConfig, PyThreadedConfig};

/// Enum of all Python config types that can be used as tree nodes.
///
/// PyO3's `FromPyObject` derive will automatically try each variant
/// when extracting from a Python object.
///
/// # Serialization (internal)
///
/// Currently uses JSON via nanoserde for cross-library transfer (e.g., pisky -> smarts).
/// JSON works for this flat enum structure, but the format is intentionally hidden
/// behind `serialize_as_bytes`/`node_from_bytes` to allow changing it later.
///
/// For any future 1.0 version, we should revisit the serialization format to handle:
/// - Namespacing (if we have nested enums or types from different modules)
/// - Versioning (for backwards compatibility)
/// - Binary formats (for efficiency)
#[derive(Clone, FromPyObject, SerJson, DeJson)]
pub enum PyNodeEnum {
    RecordReaderConfig(PyRecordReaderConfig),
    RoundRobinConfig(PyRoundRobinConfig),
    SamplingConfig(PySamplingConfig),
    ShuffleConfig(PyShuffleConfig),
    ThreadedConfig(PyThreadedConfig),
    // Shard readers (2 configs that accept order objects)
    SequentialReaderConfig(PySequentialReaderConfig),
    RoundRobinReaderConfig(PyRoundRobinReaderConfig),
    // Multi-threaded reader (1 config that accepts order object)
    MultiThreadedReaderConfig(PyMultiThreadedReaderConfig),
}

// From implementations for wrapping configs in the enum for serialization
impl From<PyRecordReaderConfig> for PyNodeEnum {
    fn from(c: PyRecordReaderConfig) -> Self {
        Self::RecordReaderConfig(c)
    }
}

impl From<PyRoundRobinConfig> for PyNodeEnum {
    fn from(c: PyRoundRobinConfig) -> Self {
        Self::RoundRobinConfig(c)
    }
}

impl From<PySamplingConfig> for PyNodeEnum {
    fn from(c: PySamplingConfig) -> Self {
        Self::SamplingConfig(c)
    }
}

impl From<PyShuffleConfig> for PyNodeEnum {
    fn from(c: PyShuffleConfig) -> Self {
        Self::ShuffleConfig(c)
    }
}

impl From<PyThreadedConfig> for PyNodeEnum {
    fn from(c: PyThreadedConfig) -> Self {
        Self::ThreadedConfig(c)
    }
}

impl From<PySequentialReaderConfig> for PyNodeEnum {
    fn from(c: PySequentialReaderConfig) -> Self {
        Self::SequentialReaderConfig(c)
    }
}

impl From<PyRoundRobinReaderConfig> for PyNodeEnum {
    fn from(c: PyRoundRobinReaderConfig) -> Self {
        Self::RoundRobinReaderConfig(c)
    }
}

impl From<PyMultiThreadedReaderConfig> for PyNodeEnum {
    fn from(c: PyMultiThreadedReaderConfig) -> Self {
        Self::MultiThreadedReaderConfig(c)
    }
}

impl Node for PyNodeEnum {
    fn make(self: Box<Self>) -> Result<Reader> {
        match *self {
            Self::RecordReaderConfig(c) => Box::new(c).make(),
            Self::RoundRobinConfig(c) => Box::new(c).make(),
            Self::SamplingConfig(c) => Box::new(c).make(),
            Self::ShuffleConfig(c) => Box::new(c).make(),
            Self::ThreadedConfig(c) => Box::new(c).make(),
            Self::SequentialReaderConfig(c) => Box::new(c).make(),
            Self::RoundRobinReaderConfig(c) => Box::new(c).make(),
            Self::MultiThreadedReaderConfig(c) => Box::new(c).make(),
        }
    }
}

/// Serialize a node config to bytes for cross-library transfer.
///
/// This is the Rust API for serializing configs (e.g., for smarts tests).
/// Hides the serialization format details.
pub fn serialize_node(node: &PyNodeEnum) -> Vec<u8> {
    node.serialize_json().into_bytes()
}

/// Active tree reader - iterates over records from a composed tree.
#[pyclass(name = "TreeReader")]
pub struct PyTreeReader {
    inner: Option<Reader>,
}

impl PyTreeReader {
    pub fn new(reader: Reader) -> Self {
        Self {
            inner: Some(reader),
        }
    }
}

#[pymethods]
impl PyTreeReader {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<Option<pyo3_bytes::PyBytes>> {
        let reader = self
            .inner
            .as_mut()
            .ok_or_else(|| PyIOError::new_err("Reader is closed"))?;

        match reader.next() {
            Some(Ok(bytes)) => Ok(Some(pyo3_bytes::PyBytes::new(bytes))),
            Some(Err(e)) => Err(PyIOError::new_err(e.to_string())),
            None => Ok(None),
        }
    }

    /// Close the reader, releasing all underlying resources.
    fn close(&mut self) {
        self.inner.take();
    }
}

/// Deserialize a node config from bytes.
///
/// Returns a `Box<dyn Node>` that can be composed or built into a reader.
/// This is the Rust API for cross-library transfer (e.g., smarts receiving a config from pisky).
pub fn node_from_bytes(serialized_config: &[u8]) -> Result<Box<dyn Node>> {
    let json_str = std::str::from_utf8(serialized_config)
        .map_err(|e| disky::error::DiskyError::Other(format!("Invalid UTF-8: {}", e)))?;

    let node: PyNodeEnum = DeJson::deserialize_json(json_str)
        .map_err(|e| disky::error::DiskyError::Other(format!("Failed to deserialize: {:?}", e)))?;

    Ok(Box::new(node))
}

/// Deserialize a node config from bytes and build a tree reader.
///
/// This is the Rust API for building a reader directly.
#[pyfunction]
pub fn tree_from_bytes(serialized_config: &[u8]) -> PyResult<PyTreeReader> {
    let node =
        node_from_bytes(serialized_config).map_err(|e| PyValueError::new_err(e.to_string()))?;

    let reader = node.make().map_err(|e| PyIOError::new_err(e.to_string()))?;

    Ok(PyTreeReader::new(reader))
}
