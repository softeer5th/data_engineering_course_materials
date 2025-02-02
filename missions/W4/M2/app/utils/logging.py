import logging
import os

from app.core.constants import LOG_DIR


class Logger:
    """
    Logger class for logging messages to a file and console.
    """

    def __init__(self, name: str, *, append: bool = True):
        """
        Initialize Logger object with log file path and mode.
        :param name: str: Name of the log file.
        :param append: bool: Append to log file if True, overwrite if False
        """
        if not name:
            raise ValueError("Name must be provided for the logger.")

        # Get or create logger with the given name
        self.logger = logging.getLogger(name)

        # If handlers are already configured, skip initialization
        if self.logger.handlers:
            return

        self.logger.setLevel(logging.DEBUG)

        # Create log directory if it doesn't exist
        os.makedirs(LOG_DIR, exist_ok=True)

        # Create and configure file handler
        mode = "a" if append else "w"
        fh = logging.FileHandler(
            os.path.join(LOG_DIR, name + ".log"), mode=mode
        )
        fh.setLevel(logging.DEBUG)

        # Create and configure console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # Create and set formatter for both handlers
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        # Add handlers to logger
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

    def debug(self, message: str):
        """
        Log debug message.
        :param message: str: Message to log.
        """
        self.logger.debug(message)

    def info(self, message: str):
        """
        Log info message.
        :param message: str: Message to log.
        """
        self.logger.info(message)

    def warning(self, message: str):
        """
        Log warning message.
        :param message: str: Message to log.
        """
        self.logger.warning(message)

    def error(self, message: str):
        """
        Log error message.
        :param message: str: Message to log.
        """
        self.logger.error(message)

    def critical(self, message: str):
        """
        Log critical message.
        :param message: str: Message to log.
        """
        self.logger.critical(message)
