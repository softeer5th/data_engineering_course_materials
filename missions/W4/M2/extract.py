from app.core.constants import VehicleType
from app.core.extractor import Extractor
from app.utils.logging import Logger


def main():
    extractor = Extractor(VehicleType.GREEN)
    extractor.extract()

    extractor = Extractor(VehicleType.YELLOW)
    extractor.extract()


if __name__ == "__main__":
    main()
