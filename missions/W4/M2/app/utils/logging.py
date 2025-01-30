import logging


class Logger:
    """
    Logger class for logging messages to a file and console.
    """

    _instance: "Logger" = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._is_initialized = False
        return cls._instance

    def __init__(self, log_file_path: str | None = None, *, append: bool = True):
        """
        Initialize Logger object with log file path and mode.
        :param log_file_path: str: Log file path.
        :param append: bool: Append to log file if True, overwrite if False
        """

        if self._is_initialized:
            if log_file_path is not None:
                raise ValueError("Logger is already initialized.")
            return
        
        self._is_initialized = True
        
        if log_file_path is None:
            raise ValueError("log_file_path is required.")

        # Create logger
        self.logger = logging.getLogger("Logger")
        self.logger.setLevel(logging.DEBUG)

        # Create file handler
        mode = "a" if append else "w"
        fh = logging.FileHandler(log_file_path, mode=mode)
        fh.setLevel(logging.DEBUG)

        # Create console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # Create formatter
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
