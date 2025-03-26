import logging
import logging.config


def configure_logger(log_level="INFO", log_to_file=False, log_file_path="app.log"):
    """
    Configures logging for the application, including setting up loggers for both console and file output.

    This function sets up the logging configuration with handlers for both console and file logging, and applies
    the specified log level and log file settings. If logging to a file is enabled, it configures the log file to
    append new log entries without overwriting the existing content.

    Args:
        log_level (str): The logging level (e.g., 'DEBUG', 'INFO', 'WARNING'). Default is 'INFO'.
        log_to_file (bool): Whether to log to a file. Default is False.
        log_file_path (str): The path to the log file where logs will be saved. Default is 'app.log'.

    Returns:
        None: This function does not return anything. It configures the logging system for the current session.
    """
    # Set up logging handlers
    handlers = {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "detailed",
            "stream": "ext://sys.stdout",
        }
    }

    # Configure file handler
    if log_to_file:
        handlers["file"] = {
            "class": "logging.FileHandler",
            "formatter": "detailed",
            "filename": log_file_path,
            "mode": "w",
        }

    # Configure the logging structure
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "detailed": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s - [%(filename)s:%(lineno)d]",
            },
        },
        "handlers": handlers,
        "root": {
            "level": log_level.upper(),
            "handlers": list(handlers.keys()),
        },
    }

    # Apply the logging configuration
    logging.config.dictConfig(logging_config)

    # Check
    logger = logging.getLogger("CONFIG")
    logger.info("Logging is configured.")
