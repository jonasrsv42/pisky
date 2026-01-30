use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use std::fs::File;
use std::path::PathBuf;

use disky::error::Result as DiskyResult;
use disky::reader::{
    CorruptionStrategy, DiskyPiece, RecordReader, RecordReaderConfig as RustReaderConfig,
    RecordReaderOptions,
};
use disky::tree::reader::{Node, Reader};

use crate::corruption::{PyCorruptionStrategy, convert_corruption_strategy};

/// Configuration for a single-file record reader.
///
/// This is a config object - it stores parameters but doesn't open any files
/// until you enter the context manager.
///
/// Example:
///     with RecordReaderConfig("data.disky") as reader:
///         for record in reader:
///             process(record)
#[pyclass(name = "RecordReaderConfig")]
#[derive(Clone)]
pub struct PyRecordReaderConfig {
    path: PathBuf,
    corruption_strategy: Option<CorruptionStrategy>,
}

#[pymethods]
impl PyRecordReaderConfig {
    #[new]
    #[pyo3(signature = (path, corruption_strategy=None))]
    fn new(path: &str, corruption_strategy: Option<PyCorruptionStrategy>) -> Self {
        Self {
            path: PathBuf::from(path),
            corruption_strategy: convert_corruption_strategy(corruption_strategy),
        }
    }

    fn __enter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyRecordReader>> {
        let config = slf.borrow(py);
        let reader = PyRecordReader::from_config(&config)?;
        Py::new(py, reader)
    }

    fn __exit__(
        &self,
        _exc_type: Option<Bound<'_, PyAny>>,
        _exc_value: Option<Bound<'_, PyAny>>,
        _traceback: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        // Reader cleanup happens via PyRecordReader's drop
        Ok(false)
    }
}

/// Active record reader - created by entering a RecordReaderConfig context.
///
/// Implements iterator protocol for reading records.
#[pyclass(name = "RecordReader")]
pub struct PyRecordReader {
    reader: Option<RecordReader<File>>,
}

impl PyRecordReader {
    fn from_config(config: &PyRecordReaderConfig) -> PyResult<Self> {
        let file = File::open(&config.path)
            .map_err(|e| PyIOError::new_err(format!("{}: {}", config.path.display(), e)))?;

        let mut builder = RustReaderConfig::new(file);

        if let Some(strategy) = config.corruption_strategy {
            let options = RecordReaderOptions::default().with_corruption_strategy(strategy);
            builder = builder.options(options);
        }

        let reader = builder
            .build()
            .map_err(|e| PyIOError::new_err(e.to_string()))?;

        Ok(Self {
            reader: Some(reader),
        })
    }
}

#[pymethods]
impl PyRecordReader {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<Option<pyo3_bytes::PyBytes>> {
        let reader = self
            .reader
            .as_mut()
            .ok_or_else(|| PyIOError::new_err("Reader is closed"))?;

        match reader.next_record() {
            Ok(DiskyPiece::Record(bytes)) => Ok(Some(pyo3_bytes::PyBytes::new(bytes))),
            Ok(DiskyPiece::EOF) => Ok(None),
            Err(e) => Err(PyIOError::new_err(e.to_string())),
        }
    }

    /// Read the next record, or None if EOF.
    fn read(&mut self) -> PyResult<Option<pyo3_bytes::PyBytes>> {
        self.__next__()
    }

    /// Close the reader, releasing the underlying file handle.
    fn close(&mut self) {
        self.reader.take();
    }
}

// Implement Node trait for tree composition
impl Node for PyRecordReaderConfig {
    fn make(self: Box<Self>) -> DiskyResult<Reader> {
        let file = File::open(&self.path)?;

        let mut builder = RustReaderConfig::new(file);

        if let Some(strategy) = self.corruption_strategy {
            let options = RecordReaderOptions::default().with_corruption_strategy(strategy);
            builder = builder.options(options);
        }

        Ok(Box::new(builder.build()?))
    }
}
