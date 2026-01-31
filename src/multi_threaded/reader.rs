//! Multi-threaded shard reader PyO3 bindings.
//!
//! Provides 2 reader configs (sequential vs random order):
//! - MultiThreadedReader + SequentialOrder: reads shards in order
//! - MultiThreadedReader + RandomOrder: reads shards in shuffled order (infinite)

use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;

use disky::parallel::multi_threaded_reader::{MultiThreadedReader, MultiThreadedReaderConfig};
use disky::parallel::reader::{DiskyParallelPiece, ParallelReaderConfig, ShardingConfig};
use disky::reader::CorruptionStrategy;
use disky::shard::source::{RandomRepeatingShardSource, SequentialShardSource};

use crate::corruption::{PyCorruptionStrategy, convert_corruption_strategy};
use crate::shard::source::PyFileShards;

// =============================================================================
// Shared reader wrapper
// =============================================================================

/// Active multi-threaded reader - iterates over records from shards in parallel.
///
/// This is the shared reader type returned by all multi-threaded reader configs.
/// Uses `&self` for all methods to allow concurrent access from multiple Python threads.
///
/// Note: We intentionally don't wrap the reader in `Option` for deterministic cleanup.
/// Using `Option` would require `&mut self` access for `close()`, which would prevent
/// concurrent reads from multiple Python threads. To support both concurrent access
/// AND deterministic cleanup, we'd need a `Mutex`, which adds overhead. Since the
/// underlying reader uses internal synchronization, we rely on Rust's `Drop` for cleanup.
#[pyclass(name = "MultiThreadedReader")]
pub struct PyMultiThreadedReader {
    reader: MultiThreadedReader<std::fs::File>,
}

impl PyMultiThreadedReader {
    pub fn new(reader: MultiThreadedReader<std::fs::File>) -> Self {
        Self { reader }
    }
}

#[pymethods]
impl PyMultiThreadedReader {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&self, py: Python<'py>) -> PyResult<Option<pyo3_bytes::PyBytes>> {
        py.allow_threads(|| {
            loop {
                match self.reader.read() {
                    Ok(DiskyParallelPiece::Record(bytes)) => {
                        return Ok(Some(pyo3_bytes::PyBytes::new(bytes)));
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
}

// =============================================================================
// Multi-threaded reader configs
// =============================================================================

/// Multi-threaded reader with sequential order.
///
/// Reads shards in parallel, in sequential order.
#[pyclass(name = "MultiThreadedReaderSequentialOrderConfig")]
#[derive(Clone)]
pub struct PyMultiThreadedReaderSeqOrderConfig {
    shards: PyFileShards,
    num_parallel: usize,
    worker_threads: Option<usize>,
    queue_size_mb: Option<usize>,
    corruption_strategy: Option<CorruptionStrategy>,
}

#[pymethods]
impl PyMultiThreadedReaderSeqOrderConfig {
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

    fn __enter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyMultiThreadedReader>> {
        let config = slf.borrow(py);
        let file_shards = config.shards.spec.build()?;
        let source = SequentialShardSource::new(file_shards);
        let sharding_config = ShardingConfig::new(Box::new(source), config.num_parallel);

        let mut parallel_reader_config = ParallelReaderConfig::default();
        if let Some(strategy) = config.corruption_strategy {
            parallel_reader_config.reader_options = parallel_reader_config
                .reader_options
                .with_corruption_strategy(strategy);
        }

        let mut mt_config = MultiThreadedReaderConfig::new(sharding_config)
            .with_reader_config(parallel_reader_config);

        if let Some(threads) = config.worker_threads {
            mt_config = mt_config.with_worker_threads(threads);
        }
        if let Some(queue_mb) = config.queue_size_mb {
            mt_config = mt_config.with_queue_size_bytes(queue_mb * 1024 * 1024);
        }

        let reader = mt_config
            .build()
            .map_err(|e| PyIOError::new_err(e.to_string()))?;

        Py::new(py, PyMultiThreadedReader::new(reader))
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

/// Multi-threaded reader with random repeating order.
///
/// Reads shards in parallel, in shuffled order. Repeats infinitely.
#[pyclass(name = "MultiThreadedReaderRandomOrderConfig")]
#[derive(Clone)]
pub struct PyMultiThreadedReaderRandOrderConfig {
    shards: PyFileShards,
    num_parallel: usize,
    worker_threads: Option<usize>,
    queue_size_mb: Option<usize>,
    corruption_strategy: Option<CorruptionStrategy>,
    seed: Option<u64>,
}

#[pymethods]
impl PyMultiThreadedReaderRandOrderConfig {
    #[new]
    #[pyo3(signature = (shards, num_parallel=2, worker_threads=None, queue_size_mb=None, corruption_strategy=None, seed=None))]
    fn new(
        shards: PyFileShards,
        num_parallel: usize,
        worker_threads: Option<usize>,
        queue_size_mb: Option<usize>,
        corruption_strategy: Option<PyCorruptionStrategy>,
        seed: Option<u64>,
    ) -> Self {
        Self {
            shards,
            num_parallel,
            worker_threads,
            queue_size_mb,
            corruption_strategy: convert_corruption_strategy(corruption_strategy),
            seed,
        }
    }

    fn __enter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyMultiThreadedReader>> {
        let config = slf.borrow(py);
        let file_shards = config.shards.spec.build()?;
        let source = match config.seed {
            Some(seed) => RandomRepeatingShardSource::with_seed(file_shards, seed),
            None => RandomRepeatingShardSource::new(file_shards),
        };
        let sharding_config = ShardingConfig::new(Box::new(source), config.num_parallel);

        let mut parallel_reader_config = ParallelReaderConfig::default();
        if let Some(strategy) = config.corruption_strategy {
            parallel_reader_config.reader_options = parallel_reader_config
                .reader_options
                .with_corruption_strategy(strategy);
        }

        let mut mt_config = MultiThreadedReaderConfig::new(sharding_config)
            .with_reader_config(parallel_reader_config);

        if let Some(threads) = config.worker_threads {
            mt_config = mt_config.with_worker_threads(threads);
        }
        if let Some(queue_mb) = config.queue_size_mb {
            mt_config = mt_config.with_queue_size_bytes(queue_mb * 1024 * 1024);
        }

        let reader = mt_config
            .build()
            .map_err(|e| PyIOError::new_err(e.to_string()))?;

        Py::new(py, PyMultiThreadedReader::new(reader))
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
