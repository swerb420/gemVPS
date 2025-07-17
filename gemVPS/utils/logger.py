# src/utils/logger.py
import logging
import sys
from logging.handlers import RotatingFileHandler
from typing import Optional


# Define a custom formatter to add color to log levels in the console
class ColorFormatter(logging.Formatter):
    """A custom log formatter that adds color to log levels."""
    GREY = "\x1b[38;20m"
    YELLOW = "\x1b[33;20m"
    RED = "\x1b[31;20m"
    BOLD_RED = "\x1b[31;1m"
    RESET = "\x1b[0m"

    # Define the format for each log level
    FORMATS = {
        logging.DEBUG: (
            GREY
            + "%(asctime)s - %(name)s - [%(levelname)s] - "
            + "%(message)s" + RESET
        ),
        logging.INFO: (
            "\x1b[32;20m"
            + "%(asctime)s - %(name)s - [%(levelname)s] - "
            + "%(message)s" + RESET
        ),  # Green for Info
        logging.WARNING: (
            YELLOW
            + "%(asctime)s - %(name)s - [%(levelname)s] - "
            + "%(message)s" + RESET
        ),
        logging.ERROR: (
            RED
            + "%(asctime)s - %(name)s - [%(levelname)s] - "
            + "%(message)s" + RESET
        ),
        logging.CRITICAL: (
            BOLD_RED
            + "%(asctime)s - %(name)s - [%(levelname)s] - "
            + "%(message)s" + RESET
        ),
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, '%Y-%m-%d %H:%M:%S')
        return formatter.format(record)


# A dictionary to cache loggers so we don't reconfigure them.
_loggers = {}


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Configures and returns a standardized logger for any module.
    It sets up both a colored console handler and a rotating file handler.

    Args:
        name (Optional[str]): The name for the logger, typically ``__name__``
            from the calling module.

    Returns:
        logging.Logger: A configured logger instance.
    """
    if name in _loggers:
        return _loggers[name]

    # Use the root logger if no name is provided
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    # Prevent log messages from being duplicated by the root logger
    logger.propagate = False

    # --- Console Handler ---
    # Use the custom color formatter for console output.
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(ColorFormatter())

    # --- File Handler ---
    # Create a rotating file handler to prevent log files from growing
    # indefinitely. This will create up to 5 backup files of 5MB each.
    file_handler = RotatingFileHandler(
        'logs/app.log',
        maxBytes=5*1024*1024,  # 5 MB
        backupCount=5
    )
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - [%(levelname)s] - %(message)s',
        '%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)

    # Add handlers to the logger only if they haven't been added before.
    if not logger.handlers:
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    _loggers[name] = logger
    return logger


# It's good practice to ensure the log directory exists at startup.
# This can be called once from main.py.
def setup_logging_directory():
    """Creates the 'logs' directory if it doesn't exist."""
    import os
    if not os.path.exists('logs'):
        os.makedirs('logs')

# Example usage in another module:
# from utils.logger import get_logger
# logger = get_logger(__name__)
# logger.info("This is an informational message.")
# logger.warning("This is a warning message.")
