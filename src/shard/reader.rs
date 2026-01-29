//! Shard reader PyO3 bindings.
//!
//! We use a macro to generate 4 concrete reader types (2 reading strategies × 2 iteration orders).

use std::fs::File;

use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;

use disky::reader::{CorruptionStrategy, RecordReaderOptions};
use disky::shard::reader::{
    RoundRobinShardReader, RoundRobinShardReaderConfig, SequentialShardReader,
    SequentialShardReaderConfig,
};
use disky::shard::source::{FileShards, RandomRepeatingShardSource, SequentialShardSource};

use crate::corruption::parse_corruption_strategy;

use super::source::PyFileShards;

/// Macro to generate a Sequential shard reader config + reader pair (infallible build).
macro_rules! define_seq {
    (
        config_name: $config_name:ident,
        config_py_name: $config_py_name:literal,
        reader_name: $reader_name:ident,
        reader_py_name: $reader_py_name:literal,
        shard_reader: $shard_reader:ident,
        shard_reader_config: $shard_reader_config:ident,
        order_source: $order_source:ident,
    ) => {
        #[pyclass(name = $config_py_name)]
        #[derive(Clone)]
        pub struct $config_name {
            shards: PyFileShards,
            corruption_strategy: Option<CorruptionStrategy>,
        }

        #[pymethods]
        impl $config_name {
            #[new]
            #[pyo3(signature = (shards, corruption_strategy=None))]
            fn new(shards: PyFileShards, corruption_strategy: Option<&str>) -> PyResult<Self> {
                Ok(Self {
                    shards,
                    corruption_strategy: parse_corruption_strategy(corruption_strategy)?,
                })
            }

            fn __enter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<$reader_name>> {
                let config = slf.borrow(py);
                let file_shards = config.shards.spec.build()?;
                let source = $order_source::new(file_shards);
                let mut builder = $shard_reader_config::new(source);
                if let Some(s) = config.corruption_strategy {
                    builder = builder.reader_options(
                        RecordReaderOptions::default().with_corruption_strategy(s),
                    );
                }
                Py::new(py, $reader_name { reader: builder.build() })
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
            reader: $shard_reader<File, $order_source<FileShards>>,
        }

        #[pymethods]
        impl $reader_name {
            fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                slf
            }

            fn __next__(&mut self) -> PyResult<Option<pyo3_bytes::PyBytes>> {
                match self.reader.next() {
                    Some(Ok(bytes)) => Ok(Some(pyo3_bytes::PyBytes::new(bytes))),
                    Some(Err(e)) => Err(PyIOError::new_err(e.to_string())),
                    None => Ok(None),
                }
            }
        }
    };
}

/// Macro to generate a RoundRobin shard reader config + reader pair (fallible build, with max_active).
macro_rules! define_rr {
    (
        config_name: $config_name:ident,
        config_py_name: $config_py_name:literal,
        reader_name: $reader_name:ident,
        reader_py_name: $reader_py_name:literal,
        shard_reader: $shard_reader:ident,
        shard_reader_config: $shard_reader_config:ident,
        order_source: $order_source:ident,
    ) => {
        #[pyclass(name = $config_py_name)]
        #[derive(Clone)]
        pub struct $config_name {
            shards: PyFileShards,
            corruption_strategy: Option<CorruptionStrategy>,
            max_active: Option<usize>,
        }

        #[pymethods]
        impl $config_name {
            #[new]
            #[pyo3(signature = (shards, corruption_strategy=None, max_active=None))]
            fn new(
                shards: PyFileShards,
                corruption_strategy: Option<&str>,
                max_active: Option<usize>,
            ) -> PyResult<Self> {
                Ok(Self {
                    shards,
                    corruption_strategy: parse_corruption_strategy(corruption_strategy)?,
                    max_active,
                })
            }

            fn __enter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<$reader_name>> {
                let config = slf.borrow(py);
                let file_shards = config.shards.spec.build()?;
                let source = $order_source::new(file_shards);
                let mut builder = $shard_reader_config::new(source);
                if let Some(s) = config.corruption_strategy {
                    builder = builder.reader_options(
                        RecordReaderOptions::default().with_corruption_strategy(s),
                    );
                }
                if let Some(max) = config.max_active {
                    builder = builder.max_active(max);
                }
                let reader = builder.build().map_err(|e| PyIOError::new_err(e.to_string()))?;
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
            reader: $shard_reader<File, $order_source<FileShards>>,
        }

        #[pymethods]
        impl $reader_name {
            fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                slf
            }

            fn __next__(&mut self) -> PyResult<Option<pyo3_bytes::PyBytes>> {
                match self.reader.next() {
                    Some(Ok(bytes)) => Ok(Some(pyo3_bytes::PyBytes::new(bytes))),
                    Some(Err(e)) => Err(PyIOError::new_err(e.to_string())),
                    None => Ok(None),
                }
            }
        }
    };
}

// Sequential reader (infallible build)
define_seq! {
    config_name: PySeqReaderSeqOrder,
    config_py_name: "SequentialReaderSequentialOrderConfig",
    reader_name: PySeqReaderSeqOrderReader,
    reader_py_name: "SequentialReaderSequentialOrder",
    shard_reader: SequentialShardReader,
    shard_reader_config: SequentialShardReaderConfig,
    order_source: SequentialShardSource,
}

define_seq! {
    config_name: PySeqReaderRandOrder,
    config_py_name: "SequentialReaderRandomOrderConfig",
    reader_name: PySeqReaderRandOrderReader,
    reader_py_name: "SequentialReaderRandomOrder",
    shard_reader: SequentialShardReader,
    shard_reader_config: SequentialShardReaderConfig,
    order_source: RandomRepeatingShardSource,
}

// RoundRobin reader (fallible build - opens shards upfront)
define_rr! {
    config_name: PyRRReaderSeqOrder,
    config_py_name: "RoundRobinReaderSequentialOrderConfig",
    reader_name: PyRRReaderSeqOrderReader,
    reader_py_name: "RoundRobinReaderSequentialOrder",
    shard_reader: RoundRobinShardReader,
    shard_reader_config: RoundRobinShardReaderConfig,
    order_source: SequentialShardSource,
}

define_rr! {
    config_name: PyRRReaderRandOrder,
    config_py_name: "RoundRobinReaderRandomOrderConfig",
    reader_name: PyRRReaderRandOrderReader,
    reader_py_name: "RoundRobinReaderRandomOrder",
    shard_reader: RoundRobinShardReader,
    shard_reader_config: RoundRobinShardReaderConfig,
    order_source: RandomRepeatingShardSource,
}
