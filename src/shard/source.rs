//! Shard source (FileShards) for reading.

use std::path::PathBuf;

use nanoserde::{DeJson, SerJson};
use pyo3::prelude::*;

use disky::reader::RecordReaderOptions;
use disky::shard::source::FileShards;

use crate::corruption::{PyCorruptionStrategy, convert_corruption_strategy};

/// How the shards were specified.
#[derive(Clone, SerJson, DeJson)]
pub enum ShardSpec {
    Prefix(String),
    Pattern { dir: String, prefix: String },
    Paths(Vec<String>),
}

impl ShardSpec {
    fn build_base(&self) -> disky::error::Result<FileShards> {
        match self {
            ShardSpec::Prefix(prefix) => FileShards::from_prefix(prefix),
            ShardSpec::Pattern { dir, prefix } => FileShards::from_pattern(dir, prefix),
            ShardSpec::Paths(paths) => {
                let pathbufs: Vec<PathBuf> = paths.iter().map(PathBuf::from).collect();
                FileShards::new(pathbufs)
            }
        }
    }
}

/// A collection of file-based shards for reading.
///
/// Example:
///     shards = FileShards.from_pattern("/data", "shard")
///     shards = FileShards.from_prefix("/data/shard")
///     shards = FileShards.from_paths(["/data/shard_0", "/data/shard_1"])
///     shards = shards.with_corruption_strategy(CorruptionStrategy.RECOVER)
#[pyclass(name = "ReaderFileShards")]
#[derive(Clone, SerJson, DeJson)]
pub struct PyFileShards {
    pub spec: ShardSpec,
    pub corruption_strategy: Option<PyCorruptionStrategy>,
}

impl PyFileShards {
    /// Convert to disky FileShards with reader options applied.
    pub fn into_disky(&self) -> disky::error::Result<FileShards> {
        let shards = self.spec.build_base()?;
        let mut options = RecordReaderOptions::default();
        if let Some(strategy) = convert_corruption_strategy(self.corruption_strategy) {
            options = options.with_corruption_strategy(strategy);
        }
        Ok(shards.reader_options(options))
    }
}

#[pymethods]
impl PyFileShards {
    /// Create from an explicit list of file paths.
    #[staticmethod]
    fn from_paths(paths: Vec<String>) -> Self {
        Self {
            spec: ShardSpec::Paths(paths),
            corruption_strategy: None,
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
            corruption_strategy: None,
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
            corruption_strategy: None,
        }
    }

    /// Set the corruption strategy for reading shards.
    ///
    /// Returns a new FileShards with the corruption strategy set.
    fn with_corruption_strategy(&self, strategy: PyCorruptionStrategy) -> Self {
        Self {
            spec: self.spec.clone(),
            corruption_strategy: Some(strategy),
        }
    }

    /// String representation showing the path.
    fn __str__(&self) -> String {
        match &self.spec {
            ShardSpec::Prefix(prefix) => prefix.clone(),
            ShardSpec::Pattern { dir, prefix } => format!("{}/{}", dir, prefix),
            ShardSpec::Paths(paths) => {
                if paths.len() <= 3 {
                    paths.join(", ")
                } else {
                    format!("{}, {} ... (+{} more)", paths[0], paths[1], paths.len() - 2)
                }
            }
        }
    }
}
