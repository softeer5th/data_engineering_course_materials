from threading import Lock, Thread

from app.utils.logging import Logger

from .constants import VehicleType
from .downloader import download_trip_data


class _SharedYearMonth:
    def __init__(self, year: int, month: int):
        self._lock = Lock()
        self.year = year
        self.month = month

    @property
    def lock(self):
        return self._lock

    def increment(self):
        self.month += 1
        if self.month > 12:
            self.year += 1
            self.month = 1


class Extractor:
    def __init__(
        self,
        vehicle_type: VehicleType,
        start_year_month: tuple[int, int] = (2019, 2),
        end_year_month: tuple[int, int] = (2024, 11),
        num_threads: int = 8,
    ):
        self.vehicle_type = vehicle_type
        self.shared = _SharedYearMonth(*start_year_month)
        self.end_year = end_year_month[0]
        self.end_month = end_year_month[1]
        self.num_threads = num_threads
        self.logger = Logger("extractor")

    def _extract_worker(self):
        while True:
            with self.shared.lock:
                year, month = self.shared.year, self.shared.month
                if year > self.end_year or (
                    year == self.end_year and month > self.end_month
                ):
                    break
                self.shared.increment()

            if not download_trip_data(self.vehicle_type, year, month):
                self.logger.error(
                    f"{self.vehicle_type}-{year}-{month:02d} 데이터 추출 실패"
                )
            self.logger.info(
                f"{self.vehicle_type}-{year}-{month:02d} 데이터 추출 완료"
            )

    def extract(self):
        threads = [
            Thread(target=self._extract_worker)
            for _ in range(self.num_threads)
        ]

        self.logger.info("Extracting data...")

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        self.logger.info("Data extraction complete")
