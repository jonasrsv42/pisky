// No need for Bytes in this module
use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use std::fs::File;
use std::path::Path;

use disky::reader::{DiskyPiece, RecordReader};
use disky::writer::RecordWriter;

use crate::corruption::PyCorruptionStrategy;

/// Python wrapper for Disky's RecordWriter
#[pyclass]
pub struct PyRecordWriter {
    writer: RecordWriter<File>,
}

#[pymethods]
impl PyRecordWriter {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let file = File::create(Path::new(path)).map_err(|e| PyIOError::new_err(e.to_string()))?;
        let writer = RecordWriter::new(file).map_err(|e| PyIOError::new_err(e.to_string()))?;

        Ok(Self { writer })
    }

    fn write_record(&mut self, data: &[u8]) -> PyResult<()> {
        self.writer
            .write_record(data)
            .map_err(|e| PyIOError::new_err(e.to_string()))
    }

    fn flush(&mut self) -> PyResult<()> {
        self.writer
            .flush()
            .map_err(|e| PyIOError::new_err(e.to_string()))
    }

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
        Ok(false) // Don't suppress exceptions
    }
}

/// Python wrapper for Disky's RecordReader
#[pyclass]
pub struct PyRecordReader {
    reader: RecordReader<File>,
}

#[pymethods]
impl PyRecordReader {
    #[new]
    fn new(path: &str, corruption_strategy: Option<PyCorruptionStrategy>) -> PyResult<Self> {
        let file = File::open(Path::new(path)).map_err(|e| PyIOError::new_err(e.to_string()))?;
        
        let reader = match corruption_strategy {
            Some(PyCorruptionStrategy::Recover) => {
                let config = disky::reader::RecordReaderConfig::default()
                    .with_corruption_strategy(disky::reader::CorruptionStrategy::Recover);
                RecordReader::with_config(file, config).map_err(|e| PyIOError::new_err(e.to_string()))?
            },
            _ => RecordReader::new(file).map_err(|e| PyIOError::new_err(e.to_string()))?,
        };

        Ok(Self { reader })
    }

    /// Count the number of records in the file
    #[staticmethod]
    fn count_records(path: &str, corruption_strategy: Option<PyCorruptionStrategy>) -> PyResult<usize> {
        let file = File::open(Path::new(path)).map_err(|e| PyIOError::new_err(e.to_string()))?;
        
        let mut reader = match corruption_strategy {
            Some(PyCorruptionStrategy::Recover) => {
                let config = disky::reader::RecordReaderConfig::default()
                    .with_corruption_strategy(disky::reader::CorruptionStrategy::Recover);
                RecordReader::with_config(file, config).map_err(|e| PyIOError::new_err(e.to_string()))?
            },
            _ => RecordReader::new(file).map_err(|e| PyIOError::new_err(e.to_string()))?,
        };

        let mut count = 0;
        loop {
            match reader.next_record() {
                Ok(DiskyPiece::Record(_)) => count += 1,
                Ok(DiskyPiece::EOF) => break,
                Err(e) => return Err(PyIOError::new_err(e.to_string())),
            }
        }

        Ok(count)
    }

    fn next_record(&mut self) -> PyResult<Option<pyo3_bytes::PyBytes>> {
        match self.reader.next_record() {
            Ok(DiskyPiece::Record(bytes)) => {
                // Create a zero-copy Python bytes object from the Rust bytes
                let py_bytes = pyo3_bytes::PyBytes::new(bytes);
                Ok(Some(py_bytes))
            }
            Ok(DiskyPiece::EOF) => Ok(None),
            Err(e) => Err(PyIOError::new_err(e.to_string())),
        }
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<Option<pyo3_bytes::PyBytes>> {
        self.next_record()
    }
}