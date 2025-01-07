import logging

class Logger:
    def __init__(self, log_file_path: str, append: bool = True):
        self.logger = logging.getLogger('ETLLogger')
        self.logger.setLevel(logging.DEBUG)

        mode = 'a' if append else 'w'
        fh = logging.FileHandler(log_file_path, mode=mode)
        fh.setLevel(logging.DEBUG)

        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

    def debug(self, message: str):
        self.logger.debug(message)

    def info(self, message: str):
        self.logger.info(message)

    def warning(self, message: str):
        self.logger.warning(message)

    def error(self, message: str):
        self.logger.error(message)

    def critical(self, message: str):
        self.logger.critical(message)