use pyo3::prelude::*;

use disky::compression::CompressionType;

/// Zstd compression with configurable level.
///
/// Example:
///     RecordWriterConfig("data.disky", compression=Zstd(3))
///     RecordWriterConfig("data.disky", compression=Zstd())  # default level 3
#[pyclass(name = "Zstd")]
#[derive(Clone, Copy)]
pub struct PyZstd {
    pub level: i32,
}

#[pymethods]
impl PyZstd {
    #[new]
    #[pyo3(signature = (level=3))]
    fn new(level: i32) -> Self {
        Self { level }
    }

    fn __repr__(&self) -> String {
        format!("Zstd({})", self.level)
    }
}

/// No compression.
///
/// Example:
///     RecordWriterConfig("data.disky", compression=Uncompressed())
#[pyclass(name = "Uncompressed")]
#[derive(Clone, Copy)]
pub struct PyUncompressed;

#[pymethods]
impl PyUncompressed {
    #[new]
    fn new() -> Self {
        Self
    }

    fn __repr__(&self) -> String {
        "Uncompressed()".to_string()
    }
}

/// Wrapper enum to accept any compression type from Python.
#[derive(Clone, Copy, FromPyObject)]
pub enum PyCompression {
    Zstd(PyZstd),
    Uncompressed(PyUncompressed),
}

impl PyCompression {
    pub fn to_rust(&self) -> CompressionType {
        match self {
            PyCompression::Zstd(z) => CompressionType::Zstd(z.level),
            PyCompression::Uncompressed(_) => CompressionType::None,
        }
    }
}

impl Default for PyCompression {
    fn default() -> Self {
        PyCompression::Uncompressed(PyUncompressed)
    }
}
