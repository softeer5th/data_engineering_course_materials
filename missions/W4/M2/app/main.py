import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from app.utils.logging import Logger
from app.core.extractor import extract

if __name__ == "__main__":
    logger = Logger("log/data_processing.log", append=True)
    extract(2019, 2, 2024, 11)