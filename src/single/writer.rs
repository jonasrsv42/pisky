use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use std::fs::File;
use std::path::PathBuf;

use disky::compression::CompressionType;
use disky::writer::{RecordWriter, RecordWriterConfig as RustWriterConfig, RecordWriterOptions};

use crate::compression::PyCompression;

/// Configuration for a single-file record writer.
///
/// This is a config object - it stores parameters but doesn't create any files
/// until you enter the context manager.
///
/// Example:
/// ```text
/// from pisky import RecordWriterConfig, Zstd, Uncompressed
///
/// # With compression
/// with RecordWriterConfig("data.disky", compression=Zstd(3)) as writer:
///     writer.write(b"hello")
///
/// # Without compression (default)
/// with RecordWriterConfig("data.disky") as writer:
///     writer.write(b"hello")
/// ```
#[pyclass(name = "RecordWriterConfig")]
#[derive(Clone)]
pub struct PyRecordWriterConfig {
    path: PathBuf,
    compression: CompressionType,
}

#[pymethods]
impl PyRecordWriterConfig {
    #[new]
    #[pyo3(signature = (path, compression=None))]
    fn new(path: &str, compression: Option<PyCompression>) -> PyResult<Self> {
        let compression_type = compression.unwrap_or_default().to_rust();

        Ok(Self {
            path: PathBuf::from(path),
            compression: compression_type,
        })
    }

    fn __enter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyRecordWriter>> {
        let config = slf.borrow(py);
        let writer = PyRecordWriter::from_config(&config)?;
        Py::new(py, writer)
    }

    fn __exit__(
        &self,
        _exc_type: Option<Bound<'_, PyAny>>,
        _exc_value: Option<Bound<'_, PyAny>>,
        _traceback: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        // Writer cleanup happens via PyRecordWriter's __exit__
        Ok(false)
    }
}

/// Active record writer - created by entering a RecordWriterConfig context.
#[pyclass(name = "RecordWriter")]
pub struct PyRecordWriter {
    writer: RecordWriter<File>,
}

impl PyRecordWriter {
    fn from_config(config: &PyRecordWriterConfig) -> PyResult<Self> {
        let file = File::create(&config.path)
            .map_err(|e| PyIOError::new_err(format!("{}: {}", config.path.display(), e)))?;

        let options = RecordWriterOptions::default().with_compression(config.compression);

        let writer = RustWriterConfig::new(file)
            .options(options)
            .build()
            .map_err(|e| PyIOError::new_err(e.to_string()))?;

        Ok(Self { writer })
    }
}

#[pymethods]
impl PyRecordWriter {
    /// Write a record to the file.
    fn write(&mut self, data: &[u8]) -> PyResult<()> {
        self.writer
            .write_record(data)
            .map_err(|e| PyIOError::new_err(e.to_string()))
    }

    /// Close the writer. Called automatically on context exit.
    fn close(&mut self) -> PyResult<()> {
        self.writer
            .close()
            .map_err(|e| PyIOError::new_err(e.to_string()))
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: Option<Bound<'_, PyAny>>,
        _exc_value: Option<Bound<'_, PyAny>>,
        _traceback: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.close()?;
        Ok(false)
    }
}
