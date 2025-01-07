import logging
from datetime import datetime
import sys

class Logger(logging.Logger):
    """
    input: 
    """

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
        file_handler = logging.FileHandler('missions/W1/M3/log/etl_project_log.txt', mode='a')
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

# 사용 예시
if __name__ == "__main__":
    # 로거 인스턴스 생성
    logger = Logger.get_logger("ETLLogger")

    # 로그 메시지 테스트
    logger.info("ETL 프로세스 시작")
    logger.debug("데이터 추출 중...")
    logger.warn("중복 데이터 발견")
    logger.error("변환 과정에서 오류 발생")

    # 싱글톤 패턴 테스트
    another_logger = Logger.get_logger("AnotherLogger")
    print(f"같은 인스턴스인가?: {logger is another_logger}")  # True가 출력되어야 함