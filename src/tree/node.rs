//! Node enum and tree reader for Python bindings.

use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;

use disky::error::Result;
use disky::tree::reader::{Node, Reader};

use crate::shard::{
    PyRRReaderRandOrderConfig, PyRRReaderSeqOrderConfig, PySeqReaderRandOrderConfig,
    PySeqReaderSeqOrderConfig,
};
use crate::single::PyRecordReaderConfig;

use super::{PyRoundRobinConfig, PyShuffleConfig};

/// Enum of all Python config types that can be used as tree nodes.
///
/// PyO3's `FromPyObject` derive will automatically try each variant
/// when extracting from a Python object.
#[derive(Clone, FromPyObject)]
pub enum PyNodeEnum {
    RecordReaderConfig(PyRecordReaderConfig),
    RoundRobinConfig(PyRoundRobinConfig),
    ShuffleConfig(PyShuffleConfig),
    // Shard readers (4 variants: 2 reading strategies × 2 iteration orders)
    SeqReaderSeqOrderConfig(PySeqReaderSeqOrderConfig),
    SeqReaderRandOrderConfig(PySeqReaderRandOrderConfig),
    RRReaderSeqOrderConfig(PyRRReaderSeqOrderConfig),
    RRReaderRandOrderConfig(PyRRReaderRandOrderConfig),
    // TODO: Add more variants as we implement them:
    // SamplingConfig(PySamplingConfig),
    // ThreadedConfig(PyThreadedConfig),
}

impl Node for PyNodeEnum {
    fn make(self: Box<Self>) -> Result<Reader> {
        match *self {
            Self::RecordReaderConfig(c) => Box::new(c).make(),
            Self::RoundRobinConfig(c) => Box::new(c).make(),
            Self::ShuffleConfig(c) => Box::new(c).make(),
            Self::SeqReaderSeqOrderConfig(c) => Box::new(c).make(),
            Self::SeqReaderRandOrderConfig(c) => Box::new(c).make(),
            Self::RRReaderSeqOrderConfig(c) => Box::new(c).make(),
            Self::RRReaderRandOrderConfig(c) => Box::new(c).make(),
        }
    }
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
