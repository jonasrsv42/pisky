//! Tree-based reader composition for Python.
//!
//! This module provides PyO3 bindings for disky's tree-based reader architecture,
//! allowing Python users to compose readers into complex pipelines.

mod node;
mod round_robin;
mod shuffle;

pub use node::PyTreeReader;
pub use round_robin::PyRoundRobinConfig;
pub use shuffle::PyShuffleConfig;
