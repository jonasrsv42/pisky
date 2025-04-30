use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use std::fs::File;
use std::path::Path;

use disky::writer::RecordWriter;

#[pyclass]
struct PyRecordWriter {
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
        self.writer.write_record(data)
            .map_err(|e| PyIOError::new_err(e.to_string()))
    }

    fn flush(&mut self) -> PyResult<()> {
        self.writer.flush()
            .map_err(|e| PyIOError::new_err(e.to_string()))
    }

    fn close(&mut self) -> PyResult<()> {
        self.writer.close()
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
        Ok(false)  // Don't suppress exceptions
    }
}

/// Python module for low-level Disky bindings
#[pymodule]
fn _pisky(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyRecordWriter>()?;
    Ok(())
}