use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;

use disky::reader::CorruptionStrategy;

/// Enum for corruption handling strategies (legacy)
#[pyclass]
#[derive(Clone, Copy)]
pub enum PyCorruptionStrategy {
    Error,
    Recover,
}

/// Parse corruption strategy from Python string.
pub fn parse_corruption_strategy(s: Option<&str>) -> PyResult<Option<CorruptionStrategy>> {
    match s {
        Some("recover") => Ok(Some(CorruptionStrategy::Recover)),
        Some("error") | None => Ok(None),
        Some(other) => Err(PyIOError::new_err(format!(
            "Unknown corruption strategy: '{}'. Use 'recover' or 'error'.",
            other
        ))),
    }
}
