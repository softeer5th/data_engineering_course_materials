import datetime
import time
from pathlib import Path

DEFAULT_LOG_FILE_PATH = Path(__file__).resolve().parent / "../log/log.txt"


class Logger:
    """
    Logger class
    """

    def __init__(self, log_file_path: str = DEFAULT_LOG_FILE_PATH):
        self.log_file_path = log_file_path

    def info(self, message: str):
        self._log("INFO", message)

    def error(self, message: str):
        self._log("ERROR", message)

    def print_separator(self):
        with open(self.log_file_path, "a") as log_file:
            log_file.write("-" * 30 + "\n")
            print("-" * 30)

    def _log(self, type: str, message: str):
        with open(self.log_file_path, "a") as log_file:
            timestamp = datetime.datetime.now()
            log_data = f'{timestamp.strftime("%Y-%b-%d-%H-%M-%S")}, {type}: {message}'
            log_file.write(log_data + "\n")
            print(log_data)


logger = Logger()


def init_logger(log_file_path: str):
    logger.log_file_path = log_file_path


class LogExecutionTime:
    """
    Execution time logger
    """

    def __init__(self, description: str):
        self.description = description

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.end_time = time.time()
        self.execution_time = self.end_time - self.start_time
        logger.info(f"{self.description} took {self.execution_time:.2f} seconds")
