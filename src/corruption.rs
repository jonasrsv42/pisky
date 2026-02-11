use nanoserde::{DeJson, SerJson};
use pyo3::prelude::*;

use disky::reader::CorruptionStrategy;

/// Enum for corruption handling strategies.
#[pyclass(name = "CorruptionStrategy", from_py_object)]
#[derive(Clone, Copy, SerJson, DeJson)]
pub enum PyCorruptionStrategy {
    /// Strict mode - raise an error on corruption.
    Strict,
    /// Recovery mode - attempt to recover from corruption.
    Recover,
}

impl PyCorruptionStrategy {
    /// Convert to disky's CorruptionStrategy.
    pub fn to_disky(self) -> CorruptionStrategy {
        match self {
            PyCorruptionStrategy::Recover => CorruptionStrategy::Recover,
            PyCorruptionStrategy::Strict => CorruptionStrategy::Error,
        }
    }
}

/// Convert Option<PyCorruptionStrategy> to Option<CorruptionStrategy>.
pub fn convert_corruption_strategy(
    strategy: Option<PyCorruptionStrategy>,
) -> Option<CorruptionStrategy> {
    strategy.map(|s| s.to_disky())
}
