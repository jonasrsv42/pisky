//! Shard reader PyO3 bindings.
//!
//! Provides 4 reader configs (2 reading strategies × 2 iteration orders):
//! - SequentialReader + SequentialOrder: drains shards in order
//! - SequentialReader + RandomOrder: drains shards in shuffled order (infinite)
//! - RoundRobinReader + SequentialOrder: interleaves shards in order
//! - RoundRobinReader + RandomOrder: interleaves shards in shuffled order (infinite)

use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;

use disky::reader::{CorruptionStrategy, RecordReaderOptions};
use disky::shard::reader::{RoundRobinShardReaderConfig, SequentialShardReaderConfig};
use disky::shard::source::{RandomRepeatingShardSource, SequentialShardSource};
use disky::tree::reader::{Node, Reader};

use crate::corruption::{PyCorruptionStrategy, convert_corruption_strategy};

use super::source::PyFileShards;

// =============================================================================
// Shared reader wrapper
// =============================================================================

/// Active shard reader - iterates over records from shards.
///
/// This is the shared reader type returned by all shard reader configs.
#[pyclass(name = "ShardReader")]
pub struct PyShardReader {
    reader: Option<Reader>,
}

impl PyShardReader {
    pub fn new(reader: Reader) -> Self {
        Self {
            reader: Some(reader),
        }
    }
}

#[pymethods]
impl PyShardReader {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<Option<pyo3_bytes::PyBytes>> {
        let reader = self
            .reader
            .as_mut()
            .ok_or_else(|| PyIOError::new_err("Reader is closed"))?;

        match reader.next() {
            Some(Ok(bytes)) => Ok(Some(pyo3_bytes::PyBytes::new(bytes))),
            Some(Err(e)) => Err(PyIOError::new_err(e.to_string())),
            None => Ok(None),
        }
    }

    /// Close the reader, releasing all underlying file handles.
    fn close(&mut self) {
        self.reader.take();
    }
}

// =============================================================================
// Sequential reader configs
// =============================================================================

/// Sequential shard reader with sequential order.
///
/// Drains each shard completely before moving to the next, in order.
#[pyclass(name = "SequentialReaderSequentialOrderConfig")]
#[derive(Clone)]
pub struct PySeqReaderSeqOrderConfig {
    shards: PyFileShards,
    corruption_strategy: Option<CorruptionStrategy>,
}

#[pymethods]
impl PySeqReaderSeqOrderConfig {
    #[new]
    #[pyo3(signature = (shards, corruption_strategy=None))]
    fn new(shards: PyFileShards, corruption_strategy: Option<PyCorruptionStrategy>) -> Self {
        Self {
            shards,
            corruption_strategy: convert_corruption_strategy(corruption_strategy),
        }
    }

    fn __enter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyShardReader>> {
        let config = slf.borrow(py);
        let file_shards = config.shards.spec.build()?;
        let source = SequentialShardSource::new(file_shards);

        let mut builder = SequentialShardReaderConfig::new(source);
        if let Some(s) = config.corruption_strategy {
            builder =
                builder.reader_options(RecordReaderOptions::default().with_corruption_strategy(s));
        }

        let reader: Reader = Box::new(builder)
            .make()
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        Py::new(py, PyShardReader::new(reader))
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

impl Node for PySeqReaderSeqOrderConfig {
    fn make(self: Box<Self>) -> disky::error::Result<Reader> {
        let file_shards = self.shards.spec.build_disky()?;
        let source = SequentialShardSource::new(file_shards);

        let mut builder = SequentialShardReaderConfig::new(source);
        if let Some(s) = self.corruption_strategy {
            builder =
                builder.reader_options(RecordReaderOptions::default().with_corruption_strategy(s));
        }

        Box::new(builder).make()
    }
}

/// Sequential shard reader with random repeating order.
///
/// Drains each shard completely before moving to the next, in shuffled order.
/// Repeats infinitely.
#[pyclass(name = "SequentialReaderRandomOrderConfig")]
#[derive(Clone)]
pub struct PySeqReaderRandOrderConfig {
    pub shards: PyFileShards,
    pub corruption_strategy: Option<CorruptionStrategy>,
    pub seed: Option<u64>,
}

#[pymethods]
impl PySeqReaderRandOrderConfig {
    #[new]
    #[pyo3(signature = (shards, corruption_strategy=None, seed=None))]
    fn new(
        shards: PyFileShards,
        corruption_strategy: Option<PyCorruptionStrategy>,
        seed: Option<u64>,
    ) -> Self {
        Self {
            shards,
            corruption_strategy: convert_corruption_strategy(corruption_strategy),
            seed,
        }
    }

    fn __enter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyShardReader>> {
        let config = slf.borrow(py);
        let file_shards = config.shards.spec.build()?;
        let source = match config.seed {
            Some(seed) => RandomRepeatingShardSource::with_seed(file_shards, seed),
            None => RandomRepeatingShardSource::new(file_shards),
        };

        let mut builder = SequentialShardReaderConfig::new(source);
        if let Some(s) = config.corruption_strategy {
            builder =
                builder.reader_options(RecordReaderOptions::default().with_corruption_strategy(s));
        }

        let reader: Reader = Box::new(builder)
            .make()
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        Py::new(py, PyShardReader::new(reader))
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

impl Node for PySeqReaderRandOrderConfig {
    fn make(self: Box<Self>) -> disky::error::Result<Reader> {
        let file_shards = self.shards.spec.build_disky()?;
        let source = match self.seed {
            Some(seed) => RandomRepeatingShardSource::with_seed(file_shards, seed),
            None => RandomRepeatingShardSource::new(file_shards),
        };

        let mut builder = SequentialShardReaderConfig::new(source);
        if let Some(s) = self.corruption_strategy {
            builder =
                builder.reader_options(RecordReaderOptions::default().with_corruption_strategy(s));
        }

        Box::new(builder).make()
    }
}

// =============================================================================
// Round-robin reader configs
// =============================================================================

/// Round-robin shard reader with sequential order.
///
/// Reads one record from each shard in rotation, in order.
#[pyclass(name = "RoundRobinReaderSequentialOrderConfig")]
#[derive(Clone)]
pub struct PyRRReaderSeqOrderConfig {
    shards: PyFileShards,
    corruption_strategy: Option<CorruptionStrategy>,
    max_active: Option<usize>,
}

#[pymethods]
impl PyRRReaderSeqOrderConfig {
    #[new]
    #[pyo3(signature = (shards, corruption_strategy=None, max_active=None))]
    fn new(
        shards: PyFileShards,
        corruption_strategy: Option<PyCorruptionStrategy>,
        max_active: Option<usize>,
    ) -> Self {
        Self {
            shards,
            corruption_strategy: convert_corruption_strategy(corruption_strategy),
            max_active,
        }
    }

    fn __enter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyShardReader>> {
        let config = slf.borrow(py);
        let file_shards = config.shards.spec.build()?;
        let source = SequentialShardSource::new(file_shards);

        let mut builder = RoundRobinShardReaderConfig::new(source);
        if let Some(s) = config.corruption_strategy {
            builder =
                builder.reader_options(RecordReaderOptions::default().with_corruption_strategy(s));
        }
        if let Some(max) = config.max_active {
            builder = builder.max_active(max);
        }

        let reader: Reader = Box::new(builder)
            .make()
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        Py::new(py, PyShardReader::new(reader))
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

impl Node for PyRRReaderSeqOrderConfig {
    fn make(self: Box<Self>) -> disky::error::Result<Reader> {
        let file_shards = self.shards.spec.build_disky()?;
        let source = SequentialShardSource::new(file_shards);

        let mut builder = RoundRobinShardReaderConfig::new(source);
        if let Some(s) = self.corruption_strategy {
            builder =
                builder.reader_options(RecordReaderOptions::default().with_corruption_strategy(s));
        }
        if let Some(max) = self.max_active {
            builder = builder.max_active(max);
        }

        Box::new(builder).make()
    }
}

/// Round-robin shard reader with random repeating order.
///
/// Reads one record from each shard in rotation, in shuffled order.
/// Repeats infinitely.
#[pyclass(name = "RoundRobinReaderRandomOrderConfig")]
#[derive(Clone)]
pub struct PyRRReaderRandOrderConfig {
    shards: PyFileShards,
    corruption_strategy: Option<CorruptionStrategy>,
    max_active: Option<usize>,
    seed: Option<u64>,
}

#[pymethods]
impl PyRRReaderRandOrderConfig {
    #[new]
    #[pyo3(signature = (shards, corruption_strategy=None, max_active=None, seed=None))]
    fn new(
        shards: PyFileShards,
        corruption_strategy: Option<PyCorruptionStrategy>,
        max_active: Option<usize>,
        seed: Option<u64>,
    ) -> Self {
        Self {
            shards,
            corruption_strategy: convert_corruption_strategy(corruption_strategy),
            max_active,
            seed,
        }
    }

    fn __enter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyShardReader>> {
        let config = slf.borrow(py);
        let file_shards = config.shards.spec.build()?;
        let source = match config.seed {
            Some(seed) => RandomRepeatingShardSource::with_seed(file_shards, seed),
            None => RandomRepeatingShardSource::new(file_shards),
        };

        let mut builder = RoundRobinShardReaderConfig::new(source);
        if let Some(s) = config.corruption_strategy {
            builder =
                builder.reader_options(RecordReaderOptions::default().with_corruption_strategy(s));
        }
        if let Some(max) = config.max_active {
            builder = builder.max_active(max);
        }

        let reader: Reader = Box::new(builder)
            .make()
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        Py::new(py, PyShardReader::new(reader))
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

impl Node for PyRRReaderRandOrderConfig {
    fn make(self: Box<Self>) -> disky::error::Result<Reader> {
        let file_shards = self.shards.spec.build_disky()?;
        let source = match self.seed {
            Some(seed) => RandomRepeatingShardSource::with_seed(file_shards, seed),
            None => RandomRepeatingShardSource::new(file_shards),
        };

        let mut builder = RoundRobinShardReaderConfig::new(source);
        if let Some(s) = self.corruption_strategy {
            builder =
                builder.reader_options(RecordReaderOptions::default().with_corruption_strategy(s));
        }
        if let Some(max) = self.max_active {
            builder = builder.max_active(max);
        }

        Box::new(builder).make()
    }
}
