//! Multi-threaded shard writer PyO3 bindings.

use std::fs::File;

use bytes::Bytes;
use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;

use disky::parallel::multi_threaded_writer::{MultiThreadedWriter, MultiThreadedWriterConfig};
use disky::parallel::writer::{ParallelWriterConfig, ShardingConfig};
use disky::writer::RecordWriterOptions;

use crate::compression::PyCompression;
use crate::shard::PyWriterFileShards;

/// Config for multi-threaded writer.
#[pyclass(name = "MultiThreadedWriterConfig")]
#[derive(Clone)]
pub struct PyMultiThreadedWriterConfig {
    shards: PyWriterFileShards,
    num_shards: usize,
    worker_threads: Option<usize>,
    max_bytes_per_writer: Option<usize>,
    task_queue_capacity: Option<usize>,
    enable_auto_sharding: bool,
    compression: Option<PyCompression>,
}

#[pymethods]
impl PyMultiThreadedWriterConfig {
    #[new]
    #[pyo3(signature = (
        shards,
        num_shards=2,
        worker_threads=None,
        max_bytes_per_writer=None,
        task_queue_capacity=None,
        enable_auto_sharding=true,
        compression=None
    ))]
    fn new(
        shards: PyWriterFileShards,
        num_shards: usize,
        worker_threads: Option<usize>,
        max_bytes_per_writer: Option<usize>,
        task_queue_capacity: Option<usize>,
        enable_auto_sharding: bool,
        compression: Option<PyCompression>,
    ) -> Self {
        Self {
            shards,
            num_shards,
            worker_threads,
            max_bytes_per_writer,
            task_queue_capacity,
            enable_auto_sharding,
            compression,
        }
    }

    fn __enter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyMultiThreadedWriterInstance>> {
        let config = slf.borrow(py);

        // Build file shards
        let file_shards = config.shards.sink.build()?;

        // Configure sharding
        let sharding_config = if config.enable_auto_sharding {
            ShardingConfig::with_auto_sharding(Box::new(file_shards), config.num_shards)
        } else {
            ShardingConfig::new(Box::new(file_shards), config.num_shards)
        };

        // Create writer options with compression
        let mut writer_options = RecordWriterOptions::default();
        if let Some(compression) = &config.compression {
            writer_options = writer_options.with_compression(compression.to_rust());
        }

        // Create parallel writer config
        let mut parallel_config = ParallelWriterConfig {
            writer_options,
            max_bytes_per_writer: config.max_bytes_per_writer,
            task_queue_capacity: None,
        };

        if let Some(capacity) = config.task_queue_capacity {
            parallel_config = parallel_config.with_task_queue_capacity(capacity);
        }

        // Create multi-threaded writer config
        let mut mt_config =
            MultiThreadedWriterConfig::new(sharding_config).with_writer_config(parallel_config);

        if let Some(threads) = config.worker_threads {
            mt_config = mt_config.with_worker_threads(threads);
        }

        // Build the writer
        let writer = mt_config
            .build()
            .map_err(|e| PyIOError::new_err(e.to_string()))?;

        Py::new(py, PyMultiThreadedWriterInstance { writer })
    }

    fn __exit__(
        &self,
        _a: Option<Bound<'_, PyAny>>,
        _b: Option<Bound<'_, PyAny>>,
        _c: Option<Bound<'_, PyAny>>,
    ) -> bool {
        false
    }
}

/// The actual multi-threaded writer instance returned by context manager.
#[pyclass(name = "MultiThreadedWriter")]
pub struct PyMultiThreadedWriterInstance {
    writer: MultiThreadedWriter<File>,
}

#[pymethods]
impl PyMultiThreadedWriterInstance {
    fn write<'py>(&self, py: Python<'py>, data: &[u8]) -> PyResult<()> {
        let bytes_data = Bytes::copy_from_slice(data);

        py.allow_threads(|| match self.writer.write_record(bytes_data) {
            Ok(promise) => {
                let _ = promise
                    .wait()
                    .map_err(|e| PyIOError::new_err(e.to_string()))?;
                Ok(())
            }
            Err(e) => Err(PyIOError::new_err(e.to_string())),
        })
    }

    fn flush<'py>(&self, py: Python<'py>) -> PyResult<()> {
        py.allow_threads(|| {
            self.writer
                .flush()
                .map_err(|e| PyIOError::new_err(e.to_string()))
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<()> {
        py.allow_threads(|| {
            self.writer
                .close()
                .map_err(|e| PyIOError::new_err(e.to_string()))
        })
    }
}
