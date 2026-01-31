//! Tree-based reader composition for Python.
//!
//! This module provides PyO3 bindings for disky's tree-based reader architecture,
//! allowing Python users to compose readers into complex pipelines.

pub mod node;
mod round_robin;
mod sampling;
pub mod shuffle;
mod threaded;

pub use node::{PyNodeEnum, PyTreeReader};
pub use round_robin::PyRoundRobinConfig;
pub use sampling::PySamplingConfig;
pub use shuffle::PyShuffleConfig;
pub use threaded::PyThreadedConfig;
