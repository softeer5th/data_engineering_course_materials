import logging
from datetime import datetime
import sys

import os

class Logger(logging.Logger):
    _instance = None

    def __new__(cls, name):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, name):
        # 중복 초기화 방지
        if not hasattr(self, '_initialized'):
            super().__init__(name)
            self._initialized = True
            self._setup_handlers()

    def _setup_handlers(self):
        # 스트림 핸들러 설정
        stream_handler = logging.StreamHandler(sys.stdout)
        self.addHandler(stream_handler)

        # 파일 핸들러 설정
        file_handler = logging.FileHandler(os.path.dirname(os.path.abspath(__file__)) + '/etl_project_log.txt', mode='a')
        self.addHandler(file_handler)

    @classmethod
    def get_logger(cls, name="default"):
        if cls._instance is None:
            cls._instance = Logger(name)
        return cls._instance

    def _log_with_time(self, level, message):
        current_time = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        formatted_message = f'[{current_time}], {message}'
        
        if level == 'INFO':
            super().info(formatted_message)
        elif level == 'WARNING':
            super().warning(formatted_message)
        elif level == 'DEBUG':
            super().debug(formatted_message)
        elif level == 'ERROR':
            super().error(formatted_message)

    def info(self, message):
        self._log_with_time('INFO', message)

    def warn(self, message):
        self._log_with_time('WARNING', message)

    def debug(self, message):
        self._log_with_time('DEBUG', message)

    def error(self, message):
        self._log_with_time('ERROR', message)