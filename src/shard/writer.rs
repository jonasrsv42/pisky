//! Shard writer PyO3 bindings.

use std::fs::File;

use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;

use disky::compression::CompressionType;
use disky::shard::sink::FileShardsBuilder;
use disky::shard::writer::SequentialShardWriterConfig;
use disky::writer::RecordWriterOptions;

use crate::compression::PyCompression;

/// How the shard sink was specified.
#[derive(Clone)]
pub enum ShardSink {
    Prefix {
        prefix: String,
        append: bool,
    },
    Pattern {
        dir: String,
        prefix: String,
        append: bool,
    },
}

impl ShardSink {
    pub fn build(&self) -> PyResult<disky::shard::sink::FileShards> {
        match self {
            ShardSink::Prefix { prefix, append } => {
                let mut builder = FileShardsBuilder::from_prefix(prefix)
                    .map_err(|e| PyIOError::new_err(e.to_string()))?;
                if *append {
                    builder = builder.append();
                }
                builder
                    .build()
                    .map_err(|e| PyIOError::new_err(e.to_string()))
            }
            ShardSink::Pattern {
                dir,
                prefix,
                append,
            } => {
                let mut builder = FileShardsBuilder::new(dir, prefix);
                if *append {
                    builder = builder.append();
                }
                builder
                    .build()
                    .map_err(|e| PyIOError::new_err(e.to_string()))
            }
        }
    }
}

/// A file-based shard factory for writing.
///
/// Creates sequentially numbered files: `{prefix}_0`, `{prefix}_1`, etc.
///
/// Example:
///     sink = FileShards.from_pattern("/data", "shard")
///     sink = FileShards.from_prefix("/data/shard")
///     sink = FileShards.from_pattern("/data", "shard", append=True)
#[pyclass(name = "WriterFileShards", from_py_object)]
#[derive(Clone)]
pub struct PyFileShards {
    pub sink: ShardSink,
}

#[pymethods]
impl PyFileShards {
    /// Create by specifying directory and prefix separately.
    ///
    /// If append=True, scans for existing shards and continues numbering.
    #[staticmethod]
    #[pyo3(signature = (dir, prefix, append=false))]
    fn from_pattern(dir: &str, prefix: &str, append: bool) -> Self {
        Self {
            sink: ShardSink::Pattern {
                dir: dir.to_string(),
                prefix: prefix.to_string(),
                append,
            },
        }
    }

    /// Create from a path prefix (e.g., "/data/shard" -> dir="/data", prefix="shard").
    ///
    /// If append=True, scans for existing shards and continues numbering.
    #[staticmethod]
    #[pyo3(signature = (prefix, append=false))]
    fn from_prefix(prefix: &str, append: bool) -> Self {
        Self {
            sink: ShardSink::Prefix {
                prefix: prefix.to_string(),
                append,
            },
        }
    }
}

/// Configuration for a sequential shard writer.
///
/// Writes records to shards, rotating to a new shard when max_shard_bytes is reached.
///
/// Example:
///     sink = FileShards.from_pattern("/data", "shard")
///     with Sequential(sink, max_shard_bytes=1_000_000_000) as writer:
///         writer.write(b"hello")
#[pyclass(name = "SequentialWriterConfig", from_py_object)]
#[derive(Clone)]
pub struct PySequentialWriterConfig {
    shards: PyFileShards,
    compression: CompressionType,
    max_shard_bytes: Option<usize>,
}

#[pymethods]
impl PySequentialWriterConfig {
    #[new]
    #[pyo3(signature = (shards, compression=None, max_shard_bytes=None))]
    fn new(
        shards: PyFileShards,
        compression: Option<PyCompression>,
        max_shard_bytes: Option<usize>,
    ) -> Self {
        Self {
            shards,
            compression: compression.unwrap_or_default().to_rust(),
            max_shard_bytes,
        }
    }

    fn __enter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PySequentialWriter>> {
        let config = slf.borrow(py);
        let writer = PySequentialWriter::from_config(&config)?;
        Py::new(py, writer)
    }

    fn __exit__(
        &self,
        _exc_type: Option<Bound<'_, PyAny>>,
        _exc_value: Option<Bound<'_, PyAny>>,
        _traceback: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        Ok(false)
    }
}

type ShardWriter =
    disky::shard::writer::SequentialShardWriter<File, disky::shard::sink::FileShards>;

/// Active sequential shard writer.
#[pyclass(name = "SequentialWriter")]
pub struct PySequentialWriter {
    writer: ShardWriter,
}

impl PySequentialWriter {
    fn from_config(config: &PySequentialWriterConfig) -> PyResult<Self> {
        let file_shards = config.shards.sink.build()?;

        let options = RecordWriterOptions::default().with_compression(config.compression);

        let mut builder = SequentialShardWriterConfig::new(file_shards).options(options);

        if let Some(max_bytes) = config.max_shard_bytes {
            builder = builder.max_shard_bytes(max_bytes);
        }

        let writer = builder.build();

        Ok(Self { writer })
    }
}

#[pymethods]
impl PySequentialWriter {
    /// Write a record.
    fn write(&mut self, data: &[u8]) -> PyResult<()> {
        self.writer
            .write_record(data)
            .map_err(|e| PyIOError::new_err(e.to_string()))
    }

    /// Close the writer.
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
        Ok(false)
    }
}
