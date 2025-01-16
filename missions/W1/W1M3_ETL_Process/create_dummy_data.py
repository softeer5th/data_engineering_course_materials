import random
import string
import sys


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


def main():
    with open("large_data.csv", "w") as f:
        row_number = 100000
        if len(sys.argv) > 1:
            row_number = int(sys.argv[1])
        for _ in range(row_number):
            f.write(
                f"{generate_random_name()},{generate_random_gdp()},{generate_random_region()}\n"
            )


if __name__ == "__main__":
    main()