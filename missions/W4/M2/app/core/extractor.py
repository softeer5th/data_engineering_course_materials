from app.utils.logging import Logger
from threading import Thread, Lock

from .downloader import download_trip_data
from .constants import VehicleType

class _SharedYearMonth:
    def __init__(self, start_year: int = 2019, start_month: int = 2, end_year: int = 2024, end_month: int = 11):
        self.lock = Lock()
        self.year = start_month
        self.month = start_year
        self.end_year = end_year
        self.end_month = end_month

    def get(self) -> tuple[int, int]:
        with self.lock:
            year, month = self.year, self.month
            self.month += 1
            if self.month > 12:
                self.year += 1
                self.month = 1
            if self.year > self.end_year or (self.year == self.end_year and self.month > self.end_month):
                return None, None
            return year, month


def extract(start_year: int = 2019, start_month: int = 2, end_year: int = 2024, end_month: int = 11):
    """
    멀티 스레딩 방식으로 2019년 2월부터 Request 오류가 날 때까지 데이터를 다운로드한다.
    공유 변수를 통해서 현재 처리 중인 가장 최신의 year-month를 공유하며 작업을 수행한다.
    """

    logger = Logger()

    year_month = _SharedYearMonth(start_year, start_month, end_year, end_month)

    def _extract():
        while True:
            year, month = year_month.get()
            if year is None:
                break
            if not download_trip_data(VehicleType.YELLOW, year, month):
                logger.error(f"{VehicleType.YELLOW}-{year}-{month:02d} 데이터 추출 실패")
                break
            if not download_trip_data(VehicleType.GREEN, year, month):
                logger.error(f"{VehicleType.GREEN}-{year}-{month:02d} 데이터 추출 실패")
                break
            logger.info(f"{year}-{month:02d} 데이터 추출 완료")

    threads = [Thread(target=_extract) for _ in range(8)]

    logger.info("Data extraction is started.")

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    logger.info("Data extraction is completed.")