//! Shard source (FileShards) for reading.

use std::path::PathBuf;

use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;

use disky::shard::source::FileShards;

/// How the shards were specified.
#[derive(Clone)]
pub enum ShardSpec {
    Prefix(String),
    Pattern { dir: String, prefix: String },
    Paths(Vec<PathBuf>),
}

impl ShardSpec {
    pub fn build(&self) -> PyResult<FileShards> {
        self.build_disky()
            .map_err(|e| PyIOError::new_err(e.to_string()))
    }

    pub fn build_disky(&self) -> disky::error::Result<FileShards> {
        match self {
            ShardSpec::Prefix(prefix) => FileShards::from_prefix(prefix),
            ShardSpec::Pattern { dir, prefix } => FileShards::from_pattern(dir, prefix),
            ShardSpec::Paths(paths) => FileShards::new(paths.clone()),
        }
    }
}

/// A collection of file-based shards for reading.
///
/// Example:
///     shards = FileShards.from_pattern("/data", "shard")
///     shards = FileShards.from_prefix("/data/shard")
///     shards = FileShards.from_paths(["/data/shard_0", "/data/shard_1"])
#[pyclass(name = "ReaderFileShards")]
#[derive(Clone)]
pub struct PyFileShards {
    pub spec: ShardSpec,
}

#[pymethods]
impl PyFileShards {
    /// Create from an explicit list of file paths.
    #[staticmethod]
    fn from_paths(paths: Vec<String>) -> Self {
        Self {
            spec: ShardSpec::Paths(paths.into_iter().map(PathBuf::from).collect()),
        }
    }

    /// Create by matching all files starting with the given path prefix.
    ///
    /// The last component of the path is used as the filename prefix.
    /// For example, `/tmp/data/shard` matches `shard_0`, `shard_1`, etc.
    #[staticmethod]
    fn from_prefix(prefix: &str) -> Self {
        Self {
            spec: ShardSpec::Prefix(prefix.to_string()),
        }
    }

    /// Create by discovering files with the given prefix in a directory.
    #[staticmethod]
    fn from_pattern(dir: &str, prefix: &str) -> Self {
        Self {
            spec: ShardSpec::Pattern {
                dir: dir.to_string(),
                prefix: prefix.to_string(),
            },
        }
    }
}
