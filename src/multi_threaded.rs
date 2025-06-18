use bytes::Bytes;
use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use std::fs::File;
use std::path::PathBuf;

use disky::parallel::multi_threaded_reader::MultiThreadedReader;
use disky::parallel::multi_threaded_writer::{MultiThreadedWriter, MultiThreadedWriterConfig};
use disky::parallel::reader::DiskyParallelPiece;
use disky::parallel::sharding::{FileShardLocator, FileSharder, FileSharderConfig, MultiPathShardLocator, RandomMultiPathShardLocator};
use disky::parallel::writer::{ParallelWriterConfig, ShardingConfig as WriterShardingConfig};
use disky::writer::RecordWriterConfig;
use disky::compression::CompressionType;

use crate::corruption::PyCorruptionStrategy;
use crate::shard_helpers::{create_multi_threaded_reader, string_paths_to_pathbufs};

/// Python wrapper for Disky's MultiThreadedWriter
#[pyclass]
pub struct PyMultiThreadedWriter {
    pub writer: MultiThreadedWriter<File>,
}

#[pymethods]
impl PyMultiThreadedWriter {
    #[staticmethod]
    #[pyo3(signature = (dir_path, prefix, num_shards, worker_threads=None, max_bytes_per_writer=None, task_queue_capacity=None, enable_auto_sharding=None, append=None, compression=None))]
    fn new_with_shards(
        dir_path: &str,
        prefix: &str,
        num_shards: usize,
        worker_threads: Option<usize>,
        max_bytes_per_writer: Option<usize>,
        task_queue_capacity: Option<usize>,
        enable_auto_sharding: Option<bool>,
        append: Option<bool>,
        compression: Option<&str>,
    ) -> PyResult<Self> {
        // Create FileSharderConfig
        let mut sharder_config = FileSharderConfig::new(prefix);

        // Set append mode if specified
        if let Some(true) = append {
            sharder_config = sharder_config.with_append(true);
        }

        // Create the FileSharder with the config
        let file_sharder = FileSharder::with_config(PathBuf::from(dir_path), sharder_config);

        // Configure the sharding with auto-sharding if enabled
        let sharding_config = if let Some(true) = enable_auto_sharding {
            WriterShardingConfig::with_auto_sharding(Box::new(file_sharder), num_shards)
        } else {
            WriterShardingConfig::new(Box::new(file_sharder), num_shards)
        };

        // Create record writer config with compression if specified
        let record_writer_config = match compression {
            Some("zstd") => RecordWriterConfig::default().with_compression(CompressionType::Zstd),
            Some("none") => RecordWriterConfig::default().with_compression(CompressionType::None),
            Some(other) => {
                return Err(PyIOError::new_err(format!("Unsupported compression type: '{}'. Supported types: 'zstd', 'none'", other)));
            },
            None => RecordWriterConfig::default(),
        };

        // Create writer config starting with compression-aware record config
        let mut writer_config = ParallelWriterConfig {
            writer_config: record_writer_config,
            max_bytes_per_writer: None,
            task_queue_capacity: None,
        };

        // Apply max_bytes_per_writer if provided - directly modify the field
        writer_config.max_bytes_per_writer = max_bytes_per_writer;

        // Apply task_queue_capacity if provided
        if let Some(capacity) = task_queue_capacity {
            writer_config = writer_config.with_task_queue_capacity(capacity);
        }

        // Create the multi-threaded writer config
        let config = if let Some(threads) = worker_threads {
            MultiThreadedWriterConfig {
                writer_config,
                worker_threads: threads,
            }
        } else {
            MultiThreadedWriterConfig {
                writer_config,
                worker_threads: MultiThreadedWriterConfig::default().worker_threads,
            }
        };

        // Create the multi-threaded writer
        let writer = MultiThreadedWriter::new(sharding_config, config)
            .map_err(|e| PyIOError::new_err(e.to_string()))?;

        Ok(Self { writer })
    }

    #[pyo3(name = "write_record")]
    fn py_write_record<'py>(&self, py: Python<'py>, data: &[u8]) -> PyResult<()> {
        // Convert to Bytes
        let bytes_data = Bytes::copy_from_slice(data);

        // Write asynchronously (non-blocking) with GIL released
        py.allow_threads(|| {
            match self.writer.write_record(bytes_data) {
                Ok(promise) => {
                    // Wait for the write to complete
                    let _ = promise
                        .wait()
                        .map_err(|e| PyIOError::new_err(e.to_string()))?;
                    Ok(())
                }
                Err(e) => Err(PyIOError::new_err(e.to_string())),
            }
        })
    }

    #[pyo3(name = "flush")]
    fn py_flush<'py>(&self, py: Python<'py>) -> PyResult<()> {
        py.allow_threads(|| {
            self.writer
                .flush()
                .map_err(|e| PyIOError::new_err(e.to_string()))
        })
    }

    #[pyo3(name = "close")]
    fn py_close<'py>(&self, py: Python<'py>) -> PyResult<()> {
        py.allow_threads(|| {
            self.writer
                .close()
                .map_err(|e| PyIOError::new_err(e.to_string()))
        })
    }

    #[pyo3(name = "pending_tasks")]
    fn py_pending_tasks<'py>(&self, py: Python<'py>) -> PyResult<usize> {
        py.allow_threads(|| {
            self.writer
                .pending_tasks()
                .map_err(|e| PyIOError::new_err(e.to_string()))
        })
    }

    #[pyo3(name = "available_writers")]
    fn py_available_writers<'py>(&self, py: Python<'py>) -> PyResult<usize> {
        py.allow_threads(|| {
            self.writer
                .available_writers()
                .map_err(|e| PyIOError::new_err(e.to_string()))
        })
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Bound<'_, PyAny>>,
        _exc_value: Option<Bound<'_, PyAny>>,
        _traceback: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.py_close(py)?;
        Ok(false) // Don't suppress exceptions
    }
}

/// Python wrapper for Disky's MultiThreadedReader
#[pyclass]
pub struct PyMultiThreadedReader {
    pub reader: MultiThreadedReader<File>,
}

#[pymethods]
impl PyMultiThreadedReader {
    #[staticmethod]
    fn new_with_shards(
        dir_path: &str,
        prefix: &str,
        num_shards: usize,
        worker_threads: Option<usize>,
        queue_size_mb: Option<usize>,
        corruption_strategy: Option<PyCorruptionStrategy>,
    ) -> PyResult<Self> {
        // Create a FileShardLocator for the sharded files
        let shard_locator = FileShardLocator::new(PathBuf::from(dir_path), prefix)
            .map_err(|e| PyIOError::new_err(e.to_string()))?;

        // Create the multi-threaded reader
        let reader = create_multi_threaded_reader(
            shard_locator,
            num_shards,
            worker_threads,
            queue_size_mb,
            corruption_strategy,
        )?;

        Ok(Self { reader })
    }
    
    /// Count the number of records in a directory with sharded files
    #[staticmethod]
    fn count_records_with_shards(
        dir_path: &str,
        prefix: &str,
        num_shards: usize,
        worker_threads: Option<usize>,
        queue_size_mb: Option<usize>,
        corruption_strategy: Option<PyCorruptionStrategy>,
    ) -> PyResult<usize> {
        // Create a FileShardLocator for the sharded files
        let shard_locator = FileShardLocator::new(PathBuf::from(dir_path), prefix)
            .map_err(|e| PyIOError::new_err(e.to_string()))?;

        // Create the multi-threaded reader
        let reader = create_multi_threaded_reader(
            shard_locator,
            num_shards,
            worker_threads,
            queue_size_mb,
            corruption_strategy,
        )?;

        let mut count = 0;
        loop {
            match reader.read() {
                Ok(DiskyParallelPiece::Record(_)) => count += 1,
                Ok(DiskyParallelPiece::EOF) => break,
                Ok(DiskyParallelPiece::ShardFinished) => continue, // Skip shard finished markers
                Err(e) => return Err(PyIOError::new_err(e.to_string())),
            }
        }

        // Close the reader explicitly
        reader.close().map_err(|e| PyIOError::new_err(e.to_string()))?;

        Ok(count)
    }
    
    #[staticmethod]
    fn new_with_shard_paths(
        shard_paths: Vec<String>,
        num_shards: usize,
        worker_threads: Option<usize>,
        queue_size_mb: Option<usize>,
        corruption_strategy: Option<PyCorruptionStrategy>,
    ) -> PyResult<Self> {
        // Convert Vec<String> to Vec<PathBuf>
        let path_bufs = string_paths_to_pathbufs(shard_paths);
            
        // Create a MultiPathShardLocator with the shard paths
        let shard_locator = MultiPathShardLocator::new(path_bufs)
            .map_err(|e| PyIOError::new_err(e.to_string()))?;

        // Create the multi-threaded reader
        let reader = create_multi_threaded_reader(
            shard_locator,
            num_shards,
            worker_threads,
            queue_size_mb,
            corruption_strategy,
        )?;

        Ok(Self { reader })
    }
    
    /// Count the number of records in a list of shard paths
    #[staticmethod]
    fn count_records_with_shard_paths(
        shard_paths: Vec<String>,
        num_shards: usize,
        worker_threads: Option<usize>,
        queue_size_mb: Option<usize>,
        corruption_strategy: Option<PyCorruptionStrategy>,
    ) -> PyResult<usize> {
        // Convert Vec<String> to Vec<PathBuf>
        let path_bufs = string_paths_to_pathbufs(shard_paths);
            
        // Create a MultiPathShardLocator with the shard paths
        let shard_locator = MultiPathShardLocator::new(path_bufs)
            .map_err(|e| PyIOError::new_err(e.to_string()))?;

        // Create the multi-threaded reader
        let reader = create_multi_threaded_reader(
            shard_locator,
            num_shards,
            worker_threads,
            queue_size_mb,
            corruption_strategy,
        )?;

        let mut count = 0;
        loop {
            match reader.read() {
                Ok(DiskyParallelPiece::Record(_)) => count += 1,
                Ok(DiskyParallelPiece::EOF) => break,
                Ok(DiskyParallelPiece::ShardFinished) => continue, // Skip shard finished markers
                Err(e) => return Err(PyIOError::new_err(e.to_string())),
            }
        }

        // Close the reader explicitly
        reader.close().map_err(|e| PyIOError::new_err(e.to_string()))?;

        Ok(count)
    }
    
    #[staticmethod]
    fn new_with_random_shard_paths(
        shard_paths: Vec<String>,
        num_shards: usize,
        worker_threads: Option<usize>,
        queue_size_mb: Option<usize>,
        corruption_strategy: Option<PyCorruptionStrategy>,
    ) -> PyResult<Self> {
        // Convert Vec<String> to Vec<PathBuf>
        let path_bufs = string_paths_to_pathbufs(shard_paths);
            
        // Create a RandomMultiPathShardLocator with the shard paths
        // This will read shards in a randomized order, repeating indefinitely and reshuffling
        // after each complete pass through all the shards
        let shard_locator = RandomMultiPathShardLocator::new(path_bufs)
            .map_err(|e| PyIOError::new_err(e.to_string()))?;

        // Create the multi-threaded reader
        let reader = create_multi_threaded_reader(
            shard_locator,
            num_shards,
            worker_threads,
            queue_size_mb,
            corruption_strategy,
        )?;

        Ok(Self { reader })
    }

    #[pyo3(name = "next_record")]
    fn py_next_record<'py>(&self, py: Python<'py>) -> PyResult<Option<pyo3_bytes::PyBytes>> {
        py.allow_threads(|| {
            // We need to handle ShardFinished markers inside the loop
            // to avoid recursive calls with GIL released
            loop {
                match self.reader.read() {
                    Ok(DiskyParallelPiece::Record(bytes)) => {
                        // Create a zero-copy Python bytes object from the Rust bytes
                        let py_bytes = pyo3_bytes::PyBytes::new(bytes);
                        return Ok(Some(py_bytes));
                    }
                    Ok(DiskyParallelPiece::EOF) => return Ok(None),
                    Ok(DiskyParallelPiece::ShardFinished) => {
                        // Skip shard finished markers and try again
                        continue;
                    }
                    Err(e) => return Err(PyIOError::new_err(e.to_string())),
                }
            }
        })
    }

    #[pyo3(name = "close")]
    fn py_close<'py>(&self, py: Python<'py>) -> PyResult<()> {
        py.allow_threads(|| {
            self.reader
                .close()
                .map_err(|e| PyIOError::new_err(e.to_string()))
        })
    }

    #[pyo3(name = "queued_records")]
    fn py_queued_records<'py>(&self, py: Python<'py>) -> PyResult<usize> {
        py.allow_threads(|| {
            self.reader
                .queued_records()
                .map_err(|e| PyIOError::new_err(e.to_string()))
        })
    }

    #[pyo3(name = "queued_bytes")]
    fn py_queued_bytes<'py>(&self, py: Python<'py>) -> PyResult<usize> {
        py.allow_threads(|| {
            self.reader
                .queued_bytes()
                .map_err(|e| PyIOError::new_err(e.to_string()))
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&self, py: Python<'py>) -> PyResult<Option<pyo3_bytes::PyBytes>> {
        self.py_next_record(py)
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Bound<'_, PyAny>>,
        _exc_value: Option<Bound<'_, PyAny>>,
        _traceback: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.py_close(py)?;
        Ok(false) // Don't suppress exceptions
    }
}
