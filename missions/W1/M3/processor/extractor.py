import json
import re
import sqlite3
from typing import Callable

import requests
from bs4 import BeautifulSoup, element

FOOTNOTE_PATTERN = re.compile(r"\[[^\]]*\]")


def _fetch_page(url: str) -> str | None:
    """
    Fetch page content.
    :param url: str: URL.
    :return: str | None: Page content
    """
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        return None


def _parse_html(html: str) -> BeautifulSoup:
    """
    Parse HTML content.
    :param html: str: HTML content.
    :return: BeautifulSoup: Parsed HTML content.
    """
    soup = BeautifulSoup(html, "html.parser")
    return soup


def _find_gdp_table(
    soup: BeautifulSoup, caption_text: str = "GDP"
) -> BeautifulSoup:
    """
    Find GDP table in HTML content.
    :param soup: BeautifulSoup: Parsed HTML content.
    :param caption_text: str: Caption text.
    :return: BeautifulSoup: GDP table.
    """

    # Find tables with class 'wikitable'
    tables: element.ResultSet[element.Tag] = soup.find_all(
        "table", class_=["wikitable"]
    )

    # Find table with caption containing 'GDP'
    for table in tables:
        if table.caption and caption_text in table.caption.text:
            return table

    return None


def _strip_footnotes(text: str) -> str:
    """
    Strip footnotes from text.
    :param text: str: Text.
    :return: str: Text without footnotes.
    """
    return FOOTNOTE_PATTERN.sub("", text)


def _get_imf_data(gdp_table: BeautifulSoup) -> dict:
    """
    Get IMF GDP data from table.
    :param gdp_table: BeautifulSoup: GDP table.
    :return: dict: IMF GDP data.
    """

    # Table columns indexes
    # May vary depending on the table structure

    # Country name, GDP, Year columns indexes
    country_index = 0
    imf_gdp_index = 1
    imf_year_index = 2
    # Row start index (skip world row)
    row_start_index = 3

    tbody: element.Tag = gdp_table.find("tbody")
    rows: element.ResultSet[element.Tag] = tbody.find_all("tr")

    # Extract data from table rows
    data = []

    for row in rows[row_start_index:]:
        columns: element.ResultSet[element.Tag] = row.find_all("td")
        # Strip footnotes from country name
        country = _strip_footnotes(columns[country_index].text).strip()

        # Check if GDP and Year columns have 'table-na' class
        if columns[imf_gdp_index].get("class") and "table-na" in columns[
            imf_gdp_index
        ].get("class"):
            gdp = None
            year = None
        else:
            gdp = _strip_footnotes(columns[imf_gdp_index].text).strip()
            year = _strip_footnotes(columns[imf_year_index].text).strip()

        # Append data to list
        data.append({"country": country, "gdp": gdp, "year": year})

    return data


def extract(wiki_url: str) -> dict:
    """
    Extract GDP data from Wikipedia page.
    :param wiki_url: str: Wikipedia URL.
    :return: dict: Extracted data.
    """
    html = _fetch_page(wiki_url)
    soup = _parse_html(html)
    gdp_table = _find_gdp_table(soup)
    extracted_data = _get_imf_data(gdp_table)

    return extracted_data
