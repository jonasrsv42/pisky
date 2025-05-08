use env_logger;
use log::LevelFilter;
use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;

/// Initialize logger with a specific logging level
pub fn init_logger(level: LevelFilter) {
    let _ = env_logger::builder()
        .filter_level(level)
        .format_timestamp(None)
        .format_target(false)
        .try_init();
}

/// Set the log level for the Disky library
#[pyfunction]
pub fn set_log_level(level_str: &str) -> PyResult<()> {
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