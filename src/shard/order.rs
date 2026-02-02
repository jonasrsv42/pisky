//! Shard iteration order strategies.

use nanoserde::{DeJson, SerJson};
use pyo3::prelude::*;

use disky::error::Result;
use disky::shard::source::{RandomRepeatingShardSource, SequentialShardSource, Shard};

use super::source::PyFileShards;

/// Type alias for boxed shard source iterator.
pub type ShardSourceIter = Box<dyn Iterator<Item = Result<Shard>> + Send + Sync>;

/// Sequential iteration over shards (in-order, finite).
#[pyclass(name = "SequentialOrder")]
#[derive(Clone, SerJson, DeJson)]
pub struct PySequentialOrder {
    pub shards: PyFileShards,
}

#[pymethods]
impl PySequentialOrder {
    #[new]
    fn new(shards: PyFileShards) -> Self {
        Self { shards }
    }
}

/// Random repeating iteration over shards (shuffled, infinite).
#[pyclass(name = "RandomRepeatOrder")]
#[derive(Clone, SerJson, DeJson)]
pub struct PyRandomRepeatOrder {
    pub shards: PyFileShards,
    pub seed: Option<u64>,
}

#[pymethods]
impl PyRandomRepeatOrder {
    #[new]
    #[pyo3(signature = (shards, seed=None))]
    fn new(shards: PyFileShards, seed: Option<u64>) -> Self {
        Self { shards, seed }
    }
}

/// Union type for shard orders - accepts either SequentialOrder or RandomRepeatOrder from Python.
#[derive(Clone, SerJson, DeJson, FromPyObject)]
pub enum ShardOrder {
    Sequential(PySequentialOrder),
    RandomRepeat(PyRandomRepeatOrder),
}

impl ShardOrder {
    /// Convert into a disky shard source iterator.
    pub fn into_source(self) -> Result<ShardSourceIter> {
        match self {
            ShardOrder::Sequential(order) => {
                let file_shards = order.shards.into_disky()?;
                Ok(Box::new(SequentialShardSource::new(file_shards)))
            }
            ShardOrder::RandomRepeat(order) => {
                let file_shards = order.shards.into_disky()?;
                let source = match order.seed {
                    Some(seed) => RandomRepeatingShardSource::with_seed(file_shards, seed),
                    None => RandomRepeatingShardSource::new(file_shards),
                };
                Ok(Box::new(source))
            }
        }
    }
}
