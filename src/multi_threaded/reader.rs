//! Multi-threaded shard reader PyO3 bindings.
//!
//! Uses macros to generate reader types for Sequential and RandomRepeat order.

use std::fs::File;

use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;

use disky::parallel::multi_threaded_reader::{MultiThreadedReader, MultiThreadedReaderConfig};
use disky::parallel::reader::{DiskyParallelPiece, ParallelReaderConfig, ShardingConfig};
use disky::reader::CorruptionStrategy;
use disky::shard::source::{RandomRepeatingShardSource, SequentialShardSource};

use crate::corruption::{PyCorruptionStrategy, convert_corruption_strategy};
use crate::shard::source::PyFileShards;

/// Macro to generate a multi-threaded reader config + reader pair.
macro_rules! define_mt_reader {
    (
        config_name: $config_name:ident,
        config_py_name: $config_py_name:literal,
        reader_name: $reader_name:ident,
        reader_py_name: $reader_py_name:literal,
        order_source: $order_source:ident,
    ) => {
        #[pyclass(name = $config_py_name)]
        #[derive(Clone)]
        pub struct $config_name {
            shards: PyFileShards,
            num_parallel: usize,
            worker_threads: Option<usize>,
            queue_size_mb: Option<usize>,
            corruption_strategy: Option<CorruptionStrategy>,
        }

        #[pymethods]
        impl $config_name {
            #[new]
            #[pyo3(signature = (shards, num_parallel=2, worker_threads=None, queue_size_mb=None, corruption_strategy=None))]
            fn new(
                shards: PyFileShards,
                num_parallel: usize,
                worker_threads: Option<usize>,
                queue_size_mb: Option<usize>,
                corruption_strategy: Option<PyCorruptionStrategy>,
            ) -> Self {
                Self {
                    shards,
                    num_parallel,
                    worker_threads,
                    queue_size_mb,
                    corruption_strategy: convert_corruption_strategy(corruption_strategy),
                }
            }

            fn __enter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<$reader_name>> {
                let config = slf.borrow(py);
                let file_shards = config.shards.spec.build()?;
                let source = $order_source::new(file_shards);

                // Create parallel reader config with corruption strategy if specified
                let mut parallel_reader_config = ParallelReaderConfig::default();
                if let Some(strategy) = config.corruption_strategy {
                    let reader_options = parallel_reader_config
                        .reader_options
                        .with_corruption_strategy(strategy);
                    parallel_reader_config.reader_options = reader_options;
                }

                // Configure the sharding
                let sharding_config = ShardingConfig::new(Box::new(source), config.num_parallel);

                // Create the reader configuration
                let mut mt_config = MultiThreadedReaderConfig::new(sharding_config)
                    .with_reader_config(parallel_reader_config);

                if let Some(threads) = config.worker_threads {
                    mt_config = mt_config.with_worker_threads(threads);
                }
                if let Some(queue_mb) = config.queue_size_mb {
                    mt_config = mt_config.with_queue_size_bytes(queue_mb * 1024 * 1024);
                }

                // Build the multi-threaded reader
                let reader = mt_config.build().map_err(|e| PyIOError::new_err(e.to_string()))?;
                Py::new(py, $reader_name { reader })
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

        #[pyclass(name = $reader_py_name)]
        pub struct $reader_name {
            reader: MultiThreadedReader<File>,
        }

        #[pymethods]
        impl $reader_name {
            fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                slf
            }

            fn __next__<'py>(&self, py: Python<'py>) -> PyResult<Option<pyo3_bytes::PyBytes>> {
                py.allow_threads(|| {
                    loop {
                        match self.reader.read() {
                            Ok(DiskyParallelPiece::Record(bytes)) => {
                                let py_bytes = pyo3_bytes::PyBytes::new(bytes);
                                return Ok(Some(py_bytes));
                            }
                            Ok(DiskyParallelPiece::EOF) => return Ok(None),
                            Ok(DiskyParallelPiece::ShardFinished) => continue,
                            Err(e) => return Err(PyIOError::new_err(e.to_string())),
                        }
                    }
                })
            }

            fn close<'py>(&self, py: Python<'py>) -> PyResult<()> {
                py.allow_threads(|| {
                    self.reader
                        .close()
                        .map_err(|e| PyIOError::new_err(e.to_string()))
                })
            }

            fn queued_records<'py>(&self, py: Python<'py>) -> PyResult<usize> {
                py.allow_threads(|| {
                    self.reader
                        .queued_records()
                        .map_err(|e| PyIOError::new_err(e.to_string()))
                })
            }

            fn queued_bytes<'py>(&self, py: Python<'py>) -> PyResult<usize> {
                py.allow_threads(|| {
                    self.reader
                        .queued_bytes()
                        .map_err(|e| PyIOError::new_err(e.to_string()))
                })
            }
        }
    };
}

// Sequential order (finite)
define_mt_reader! {
    config_name: PyMultiThreadedReaderSeqOrder,
    config_py_name: "MultiThreadedReaderSequentialOrderConfig",
    reader_name: PyMultiThreadedReaderSeqOrderReader,
    reader_py_name: "MultiThreadedReaderSequentialOrder",
    order_source: SequentialShardSource,
}

// Random repeating order (infinite)
define_mt_reader! {
    config_name: PyMultiThreadedReaderRandOrder,
    config_py_name: "MultiThreadedReaderRandomOrderConfig",
    reader_name: PyMultiThreadedReaderRandOrderReader,
    reader_py_name: "MultiThreadedReaderRandomOrder",
    order_source: RandomRepeatingShardSource,
}
