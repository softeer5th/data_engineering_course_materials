import json
import requests
from bs4 import BeautifulSoup
from pathlib import Path

TARGET_URL = "https://en.wikipedia.org/wiki/List_of_sovereign_states_and_dependent_territories_by_continent"
OUTPUT_FILE_PATH = "../data/country_region_table.json"


def save_to_json(data, file_path):
    home_dir = Path(__file__).resolve().parent
    out_dir = home_dir / file_path

    with open(out_dir, "w") as file:
        json.dump(data, file, indent=2)


def main():
    """
    Extract country and region from wikipedia
    """
    response = requests.get(TARGET_URL)
    soup = BeautifulSoup(response.text, "html.parser")
    tables = soup.findAll("table", class_="wikitable")[:6]
    regions = ["Africa", "Asia", "Europe", "North America", "Oceania", "South America"]
    country_region_table = {}
    for table, region in zip(tables, regions):
        rows = table.findAll("tr")
        for row in rows:
            td = row.find("td")
            if not td:
                continue
            b = td.find("b")
            if not b:
                continue
            a = b.find("a")
            if not a:
                continue
            countryName = a["title"]
            countryNameAlias = a.text.strip()
            country_region_table[countryName] = region
            country_region_table[countryNameAlias] = region

    # manually add
    country_region_table["DR Congo"] = "Africa"
    country_region_table["Congo"] = "Africa"
    country_region_table["Bahamas"] = "North America"
    country_region_table["Gambia"] = "Africa"

    save_to_json(country_region_table, OUTPUT_FILE_PATH)


if __name__ == "__main__":
    main()
