use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use std::fs::File;
use std::path::PathBuf;

use disky::error::Result as DiskyResult;
use disky::parallel::multi_threaded_reader::{MultiThreadedReader, MultiThreadedReaderConfig};
use disky::parallel::reader::{ParallelReaderConfig, ShardingConfig};
use disky::shard::source::{FileShards, RandomRepeatingShardSource, SequentialShardSource, Shard};

use crate::corruption::PyCorruptionStrategy;

/// Helper function to create a MultiThreadedReader from a shard iterator
pub fn create_multi_threaded_reader(
    shard_source: Box<dyn Iterator<Item = DiskyResult<Shard<File>>> + Send>,
    num_shards: usize,
    worker_threads: Option<usize>,
    queue_size_mb: Option<usize>,
    corruption_strategy: Option<PyCorruptionStrategy>,
) -> PyResult<MultiThreadedReader<File>> {
    // Create the parallel reader configuration with corruption strategy if specified
    let mut parallel_reader_config = ParallelReaderConfig::default();

    // Set corruption strategy if specified
    if let Some(PyCorruptionStrategy::Recover) = corruption_strategy {
        let reader_options = parallel_reader_config
            .reader_options
            .with_corruption_strategy(disky::reader::CorruptionStrategy::Recover);
        parallel_reader_config.reader_options = reader_options;
    }

    // Configure the sharding
    let sharding_config = ShardingConfig::new(shard_source, num_shards);

    // Create the reader configuration using builder pattern
    let mut config = MultiThreadedReaderConfig::new(sharding_config)
        .with_reader_config(parallel_reader_config);

    if let Some(threads) = worker_threads {
        config = config.with_worker_threads(threads);
    }
    if let Some(queue_mb) = queue_size_mb {
        config = config.with_queue_size_bytes(queue_mb * 1024 * 1024);
    }

    // Build the multi-threaded reader
    config.build().map_err(|e| PyIOError::new_err(e.to_string()))
}

/// Create a sequential shard source from directory + prefix
pub fn create_sequential_source(
    dir_path: &str,
    prefix: &str,
) -> PyResult<Box<dyn Iterator<Item = DiskyResult<Shard<File>>> + Send>> {
    let file_shards = FileShards::from_pattern(dir_path, prefix)
        .map_err(|e| PyIOError::new_err(e.to_string()))?;
    Ok(Box::new(SequentialShardSource::new(file_shards)))
}

/// Create a sequential shard source from explicit paths
pub fn create_sequential_source_from_paths(
    paths: Vec<PathBuf>,
) -> PyResult<Box<dyn Iterator<Item = DiskyResult<Shard<File>>> + Send>> {
    let file_shards = FileShards::new(paths)
        .map_err(|e| PyIOError::new_err(e.to_string()))?;
    Ok(Box::new(SequentialShardSource::new(file_shards)))
}

/// Create a random repeating shard source from explicit paths (infinite iteration)
pub fn create_random_repeating_source(
    paths: Vec<PathBuf>,
) -> PyResult<Box<dyn Iterator<Item = DiskyResult<Shard<File>>> + Send>> {
    let file_shards = FileShards::new(paths)
        .map_err(|e| PyIOError::new_err(e.to_string()))?;
    Ok(Box::new(RandomRepeatingShardSource::new(file_shards)))
}

/// Helper function to convert a vector of string paths to PathBufs
pub fn string_paths_to_pathbufs(shard_paths: Vec<String>) -> Vec<PathBuf> {
    shard_paths.into_iter().map(|s| PathBuf::from(s)).collect()
}
