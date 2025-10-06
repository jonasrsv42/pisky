use pyo3::prelude::*;

/// Enum for corruption handling strategies
#[pyclass]
#[derive(Clone, Copy)]
pub enum PyCorruptionStrategy {
    Error,
    Recover,
}
