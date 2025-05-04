use bytes::Bytes;
use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use std::fs::File;
use std::path::{Path, PathBuf};
use env_logger;
use log::LevelFilter;

use disky::reader::{DiskyPiece, RecordReader};
use disky::writer::RecordWriter;

use disky::parallel::multi_threaded_reader::{MultiThreadedReader, MultiThreadedReaderConfig};
use disky::parallel::multi_threaded_writer::{MultiThreadedWriter, MultiThreadedWriterConfig};
use disky::parallel::reader::{
    DiskyParallelPiece, ParallelReaderConfig, ShardingConfig as ReaderShardingConfig,
};
use disky::parallel::sharding::{FileShardLocator, FileSharder, FileSharderConfig};
use disky::parallel::writer::{ParallelWriterConfig, ShardingConfig as WriterShardingConfig};

#[pyclass]
struct PyRecordWriter {
    writer: RecordWriter<File>,
}

#[pymethods]
impl PyRecordWriter {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let file = File::create(Path::new(path)).map_err(|e| PyIOError::new_err(e.to_string()))?;
        let writer = RecordWriter::new(file).map_err(|e| PyIOError::new_err(e.to_string()))?;

        Ok(Self { writer })
    }

    fn write_record(&mut self, data: &[u8]) -> PyResult<()> {
        self.writer
            .write_record(data)
            .map_err(|e| PyIOError::new_err(e.to_string()))
    }

    fn flush(&mut self) -> PyResult<()> {
        self.writer
            .flush()
            .map_err(|e| PyIOError::new_err(e.to_string()))
    }

    fn close(&mut self) -> PyResult<()> {
        self.writer
            .close()
            .map_err(|e| PyIOError::new_err(e.to_string()))
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: Option<Bound<'_, PyAny>>,
        _exc_value: Option<Bound<'_, PyAny>>,
        _traceback: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.close()?;
        Ok(false) // Don't suppress exceptions
    }
}

#[pyclass]
#[derive(Clone, Copy)]
enum PyCorruptionStrategy {
    Error,
    Recover,
}

#[pyclass]
struct PyRecordReader {
    reader: RecordReader<File>,
}

#[pymethods]
impl PyRecordReader {
    #[new]
    fn new(path: &str, corruption_strategy: Option<PyCorruptionStrategy>) -> PyResult<Self> {
        let file = File::open(Path::new(path)).map_err(|e| PyIOError::new_err(e.to_string()))?;
        
        let reader = match corruption_strategy {
            Some(PyCorruptionStrategy::Recover) => {
                let config = disky::reader::RecordReaderConfig::default()
                    .with_corruption_strategy(disky::reader::CorruptionStrategy::Recover);
                RecordReader::with_config(file, config).map_err(|e| PyIOError::new_err(e.to_string()))?
            },
            _ => RecordReader::new(file).map_err(|e| PyIOError::new_err(e.to_string()))?,
        };

        Ok(Self { reader })
    }

    fn next_record(&mut self) -> PyResult<Option<pyo3_bytes::PyBytes>> {
        match self.reader.next_record() {
            Ok(DiskyPiece::Record(bytes)) => {
                // Create a zero-copy Python bytes object from the Rust bytes
                let py_bytes = pyo3_bytes::PyBytes::new(bytes);
                Ok(Some(py_bytes))
            }
            Ok(DiskyPiece::EOF) => Ok(None),
            Err(e) => Err(PyIOError::new_err(e.to_string())),
        }
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<Option<pyo3_bytes::PyBytes>> {
        self.next_record()
    }
}

#[pyclass]
struct PyMultiThreadedWriter {
    writer: MultiThreadedWriter<File>,
}

#[pymethods]
impl PyMultiThreadedWriter {
    #[staticmethod]
    fn new_with_shards(
        dir_path: &str,
        prefix: &str,
        num_shards: usize,
        worker_threads: Option<usize>,
        max_bytes_per_writer: Option<usize>,
        task_queue_capacity: Option<usize>,
        enable_auto_sharding: Option<bool>,
        append: Option<bool>,
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

        // Create writer config starting with default
        let mut writer_config = ParallelWriterConfig::default();

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

#[pyclass]
struct PyMultiThreadedReader {
    reader: MultiThreadedReader<File>,
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

        // Configure the sharding
        let sharding_config = ReaderShardingConfig::new(Box::new(shard_locator), num_shards);

        // Create the parallel reader configuration with corruption strategy if specified
        let mut parallel_reader_config = ParallelReaderConfig::default();
        
        // Set corruption strategy if specified
        if let Some(PyCorruptionStrategy::Recover) = corruption_strategy {
            // Update the reader_config with the corruption strategy
            let reader_config = parallel_reader_config.reader_config
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
                }
            }
            (Some(threads), None) => {
                MultiThreadedReaderConfig {
                    reader_config: parallel_reader_config,
                    worker_threads: threads,
                    queue_size_bytes: 8 * 1024 * 1024, // Default 8MB
                }
            }
            (None, Some(queue_mb)) => {
                MultiThreadedReaderConfig {
                    reader_config: parallel_reader_config,
                    worker_threads: MultiThreadedReaderConfig::default().worker_threads,
                    queue_size_bytes: queue_mb * 1024 * 1024, // Convert MB to bytes
                }
            }
            (None, None) => {
                let mut config = MultiThreadedReaderConfig::default();
                config.reader_config = parallel_reader_config;
                config
            },
        };

        // Create the multi-threaded reader
        let reader = MultiThreadedReader::new(sharding_config, config)
            .map_err(|e| PyIOError::new_err(e.to_string()))?;

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

/// Initialize logger with a specific logging level
fn init_logger(level: LevelFilter) {
    let _ = env_logger::builder()
        .filter_level(level)
        .format_timestamp(None)
        .format_target(false)
        .try_init();
}

/// Python module for low-level Disky bindings
#[pymodule]
fn _pisky(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialize logger with info level by default
    init_logger(LevelFilter::Info);
    
    m.add_class::<PyRecordWriter>()?;
    m.add_class::<PyRecordReader>()?;
    m.add_class::<PyMultiThreadedWriter>()?;
    m.add_class::<PyMultiThreadedReader>()?;
    m.add_class::<PyCorruptionStrategy>()?;

    // Add function to set log level
    #[pyfn(m)]
    fn set_log_level(level_str: &str) -> PyResult<()> {
        let level = match level_str.to_lowercase().as_str() {
            "trace" => LevelFilter::Trace,
            "debug" => LevelFilter::Debug,
            "info" => LevelFilter::Info,
            "warn" | "warning" => LevelFilter::Warn,
            "error" => LevelFilter::Error,
            "off" => LevelFilter::Off,
            _ => {
                return Err(PyIOError::new_err(format!(
                    "Invalid log level: {}. Valid levels are: trace, debug, info, warn, error, off",
                    level_str
                )))
            }
        };
        
        init_logger(level);
        Ok(())
    }

    Ok(())
}
