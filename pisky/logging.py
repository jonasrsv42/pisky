"""Logging configuration for the Pisky library."""

from ._pisky import set_log_level as _set_log_level


def set_log_level(level_str: str) -> None:
    """
    Set the logging level for the Disky library.

    Args:
        level_str: One of "trace", "debug", "info", "warn", "error", or "off"

    Raises:
        IOError: If an invalid log level is provided
    """
    _set_log_level(level_str)
