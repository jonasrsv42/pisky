use pyo3::prelude::*;

use disky::reader::CorruptionStrategy;

/// Enum for corruption handling strategies.
#[pyclass]
#[derive(Clone, Copy)]
pub enum PyCorruptionStrategy {
    Error,
    Recover,
}

/// Convert PyCorruptionStrategy to disky's CorruptionStrategy.
pub fn convert_corruption_strategy(
    strategy: Option<PyCorruptionStrategy>,
) -> Option<CorruptionStrategy> {
    match strategy {
        Some(PyCorruptionStrategy::Recover) => Some(CorruptionStrategy::Recover),
        Some(PyCorruptionStrategy::Error) | None => None,
    }
}
