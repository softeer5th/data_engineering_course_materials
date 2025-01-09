import random
import string
import sys
import time
from multiprocessing import Pool
from pathlib import Path

OUTPUT_FILE_PATH = "../data/large_data.csv"


def generate_random_name():
    """
    name format: 3~10 characters
    first letter should be uppercase and the rest should be lowercase
    """
    return random.choice(string.ascii_uppercase) + "".join(
        random.choices(string.ascii_lowercase, k=random.randint(2, 9))
    )


def generate_random_gdp():
    """
    gdp format: 1,000,000,000
    max value: 10,000,000
    less number, more posibility
    """
    max_value = 10000000
    k = random.choice([max_value, max_value * 0.1, max_value * 0.01])
    random_value = int(k * random.random() ** 1.8)
    return f'"{random_value:,}"'


def generate_random_region():
    return random.choice(
        ["Asia", "Europe", "Africa", "North America", "South America", "Oceania"]
    )


def generate_data():
    return (
        f"{generate_random_name()},{generate_random_gdp()},{generate_random_region()}\n"
    )


def generate_data_chunk(chunk_size: int):
    return [generate_data() for _ in range(chunk_size)]


def main():
    CHUNK_SIZE = 10000
    row_number = 10000
    if len(sys.argv) > 1:
        row_number = int(sys.argv[1])

    num_chunks = row_number // CHUNK_SIZE

    home_dir = Path(__file__).resolve().parent
    out_dir = home_dir / OUTPUT_FILE_PATH

    with open(out_dir, "w") as f:
        with Pool() as pool:
            for data_chunk in pool.imap_unordered(
                generate_data_chunk, [CHUNK_SIZE] * num_chunks
            ):
                f.writelines(data_chunk)


if __name__ == "__main__":
    time_start = time.time()
    main()
    time_end = time.time()
    print(f"Time taken: {time_end - time_start:.2f} seconds")
