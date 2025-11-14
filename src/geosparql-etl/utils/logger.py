"""Logging configuration and utilities."""

import logging
import sys
from pathlib import Path
from typing import Optional


def setup_logger(
    name: str,
    log_file: Optional[str] = None,
    level: int = logging.INFO,
    format_string: Optional[str] = None,
    console_output: bool = True,
) -> logging.Logger:
    """Set up a logger with file and/or console handlers.

    Args:
        name: Logger name
        log_file: Path to log file (optional)
        level: Logging level (default: INFO)
        format_string: Custom format string (optional)
        console_output: Whether to output to console (default: True)

    Returns:
        logging.Logger: Configured logger instance

    Example:
        from utils.logger import setup_logger

        logger = setup_logger("ETL", log_file="etl.log")
        logger.info("Starting ETL process")
        logger.error("An error occurred")
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()

    # Create formatter
    if format_string is None:
        format_string = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    formatter = logging.Formatter(format_string)

    # Add file handler if log_file specified
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Add console handler if requested
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


def setup_basic_logger(name: str = "App", level: int = logging.INFO) -> logging.Logger:
    """Set up a basic logger with console output only.

    Args:
        name: Logger name (default: "App")
        level: Logging level (default: INFO)

    Returns:
        logging.Logger: Configured logger instance

    Example:
        logger = setup_basic_logger()
        logger.info("This is a log message")
    """
    return setup_logger(name, log_file=None, level=level, console_output=True)


def setup_etl_logger(
    log_file: str = "etl.log", level: int = logging.INFO
) -> logging.Logger:
    """Set up a logger specifically for ETL operations.

    Args:
        log_file: Path to log file (default: "etl.log")
        level: Logging level (default: INFO)

    Returns:
        logging.Logger: Configured logger instance

    Example:
        logger = setup_etl_logger("my_etl.log")
        logger.info("Processing batch 1")
    """
    return setup_logger(
        "ETL",
        log_file=log_file,
        level=level,
        format_string="%(asctime)s [%(levelname)s] %(message)s",
        console_output=True,
    )


class LoggerContext:
    """Context manager for temporary logger configuration.

    Example:
        with LoggerContext("MyLogger", level=logging.DEBUG) as logger:
            logger.debug("This is a debug message")
    """

    def __init__(
        self,
        name: str,
        log_file: Optional[str] = None,
        level: int = logging.INFO,
        console_output: bool = True,
    ):
        """Initialize logger context.

        Args:
            name: Logger name
            log_file: Path to log file (optional)
            level: Logging level
            console_output: Whether to output to console
        """
        self.name = name
        self.log_file = log_file
        self.level = level
        self.console_output = console_output
        self.logger: Optional[logging.Logger] = None

    def __enter__(self) -> logging.Logger:
        """Set up and return logger."""
        self.logger = setup_logger(
            self.name,
            log_file=self.log_file,
            level=self.level,
            console_output=self.console_output,
        )
        return self.logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up logger handlers."""
        if self.logger:
            for handler in self.logger.handlers[:]:
                handler.close()
                self.logger.removeHandler(handler)
        return False


def get_logger(name: str) -> logging.Logger:
    """Get an existing logger or create a basic one if it doesn't exist.

    Args:
        name: Logger name

    Returns:
        logging.Logger: Logger instance

    Example:
        logger = get_logger(__name__)
        logger.info("Using logger")
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        # Set up basic console logging if no handlers exist
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
