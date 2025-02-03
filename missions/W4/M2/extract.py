from app.core.constants import VehicleType
from app.core.extractor import Extractor
from app.utils.logging import Logger


def main():
    extractor = Extractor(VehicleType.YELLOW, (2024, 1), (2024, 11))
    extractor.extract()


if __name__ == "__main__":
    main()
