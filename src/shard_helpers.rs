use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use std::fs::File;
use std::path::PathBuf;

use disky::parallel::multi_threaded_reader::{
    MultiThreadedReader, MultiThreadedReaderConfig, ReadingOrder,
};
use disky::parallel::reader::{ParallelReaderConfig, ShardingConfig as ReaderShardingConfig};
use disky::parallel::sharding::ShardLocator;

use crate::corruption::PyCorruptionStrategy;

/// Helper function to create a MultiThreadedReader from a shard locator
pub fn create_multi_threaded_reader<ShardLocatorType>(
    shard_locator: ShardLocatorType,
    num_shards: usize,
    worker_threads: Option<usize>,
    queue_size_mb: Option<usize>,
    corruption_strategy: Option<PyCorruptionStrategy>,
) -> PyResult<MultiThreadedReader<File>>
where
    ShardLocatorType: ShardLocator<File> + 'static + Send + Sync,
{
    // Create the parallel reader configuration with corruption strategy if specified
    let mut parallel_reader_config = ParallelReaderConfig::default();

    // Set corruption strategy if specified
    if let Some(PyCorruptionStrategy::Recover) = corruption_strategy {
        // Update the reader_config with the corruption strategy
        let reader_config = parallel_reader_config
            .reader_config
            .with_corruption_strategy(disky::reader::CorruptionStrategy::Recover);
        parallel_reader_config.reader_config = reader_config;
    }

    // Create the reader configuration
    let config = match (worker_threads, queue_size_mb) {
        (Some(threads), Some(queue_mb)) => {
            MultiThreadedReaderConfig {
                reader_config: parallel_reader_config,
                worker_threads: threads,
                queue_size_bytes: queue_mb * 1024 * 1024, // Convert MB to bytes
                reading_order: ReadingOrder::RoundRobin,
            }
        }
        (Some(threads), None) => {
            MultiThreadedReaderConfig {
                reader_config: parallel_reader_config,
                worker_threads: threads,
                queue_size_bytes: 8 * 1024 * 1024, // Default 8MB
                reading_order: ReadingOrder::RoundRobin,
            }
        }
        (None, Some(queue_mb)) => {
            MultiThreadedReaderConfig {
                reader_config: parallel_reader_config,
                worker_threads: MultiThreadedReaderConfig::default().worker_threads,
                queue_size_bytes: queue_mb * 1024 * 1024, // Convert MB to bytes
                reading_order: ReadingOrder::RoundRobin,
            }
        }
        (None, None) => {
            let mut config = MultiThreadedReaderConfig::default();
            config.reader_config = parallel_reader_config;
            config
        }
    };

    // Configure the sharding
    let sharding_config = ReaderShardingConfig::new(Box::new(shard_locator), num_shards);

    // Create the multi-threaded reader
    MultiThreadedReader::new(sharding_config, config).map_err(|e| PyIOError::new_err(e.to_string()))
}

/// Helper function to convert a vector of string paths to PathBufs
pub fn string_paths_to_pathbufs(shard_paths: Vec<String>) -> Vec<PathBuf> {
    shard_paths.into_iter().map(|s| PathBuf::from(s)).collect()
}

