//! Multi-threaded shard reader PyO3 bindings.
//!
//! Provides a single reader config that accepts order objects:
//! - MultiThreadedReaderConfig: reads shards in parallel using specified order

use nanoserde::{DeJson, SerJson};
use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;

use disky::parallel::multi_threaded_reader::{MultiThreadedReader, MultiThreadedReaderConfig};
use disky::parallel::reader::{DiskyParallelPiece, ParallelReaderConfig, ShardingConfig};
use disky::tree::reader::{Node, Reader};

use crate::shard::order::ShardOrder;
use crate::tree::node::PyNodeEnum;

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
    reader: MultiThreadedReader,
}

impl PyMultiThreadedReader {
    pub fn new(reader: MultiThreadedReader) -> Self {
        Self { reader }
    }
}

#[pymethods]
impl PyMultiThreadedReader {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&self, py: Python<'py>) -> PyResult<Option<pyo3_bytes::PyBytes>> {
        py.detach(|| {
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
        py.detach(|| {
            self.reader
                .close()
                .map_err(|e| PyIOError::new_err(e.to_string()))
        })
    }
}

// =============================================================================
// Multi-threaded reader config
// =============================================================================

/// Multi-threaded reader - reads shards in parallel using background threads.
///
/// Example:
///     shards = FileShards.from_pattern("/data", "shard")
///     order = SequentialOrder(shards)
///     with MultiThreadedReaderConfig(order, num_parallel=4) as reader:
///         for record in reader:
///             process(record)
#[pyclass(name = "MultiThreadedReaderConfig", from_py_object)]
#[derive(Clone, SerJson, DeJson)]
pub struct PyMultiThreadedReaderConfig {
    pub order: ShardOrder,
    pub num_parallel: usize,
    pub worker_threads: Option<usize>,
    pub queue_size_mb: Option<usize>,
}

impl PyMultiThreadedReaderConfig {
    fn build_reader(&self) -> disky::error::Result<MultiThreadedReader> {
        let source = self.order.clone().into_source()?;
        let sharding_config = ShardingConfig::new(source, self.num_parallel);

        let mut mt_config = MultiThreadedReaderConfig::new(sharding_config)
            .with_reader_config(ParallelReaderConfig::new());

        if let Some(threads) = self.worker_threads {
            mt_config = mt_config.with_worker_threads(threads);
        }
        if let Some(queue_mb) = self.queue_size_mb {
            mt_config = mt_config.with_queue_size_bytes(queue_mb * 1024 * 1024);
        }

        mt_config.build()
    }
}

#[pymethods]
impl PyMultiThreadedReaderConfig {
    #[new]
    #[pyo3(signature = (order, num_parallel=2, worker_threads=None, queue_size_mb=None))]
    fn new(
        order: ShardOrder,
        num_parallel: usize,
        worker_threads: Option<usize>,
        queue_size_mb: Option<usize>,
    ) -> Self {
        Self {
            order,
            num_parallel,
            worker_threads,
            queue_size_mb,
        }
    }

    fn __enter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyMultiThreadedReader>> {
        let config = slf.borrow(py);
        let reader = config
            .build_reader()
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

    /// Serialize this config to bytes for cross-library transfer.
    fn serialize_as_bytes(&self) -> Vec<u8> {
        PyNodeEnum::from(self.clone()).serialize_json().into_bytes()
    }
}

impl Node for PyMultiThreadedReaderConfig {
    fn make(self: Box<Self>) -> disky::error::Result<Reader> {
        Ok(Box::new(self.build_reader()?))
    }
}
