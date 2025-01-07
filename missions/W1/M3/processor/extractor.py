import json
import requests
from bs4 import BeautifulSoup, element
import re

def _fetch_page(url: str) -> str | None:
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        return None

def _parse_html(html: str) -> BeautifulSoup:
    soup = BeautifulSoup(html, 'html.parser')
    return soup

def _find_gdp_table(soup: BeautifulSoup, caption_text: str="GDP") -> BeautifulSoup:
    tables: element.ResultSet[element.Tag] = soup.find_all('table', class_=['wikitable'])
    
    for table in tables:
        if table.caption and caption_text in table.caption.text:
            return table
        
    return None

def _strip_footnotes(text: str) -> str:
    return re.sub(r'\[[^\]]*\]', '', text)

def _get_imf_data(gdp_table: BeautifulSoup) -> dict:
    country_index = 0
    imf_gdp_index = 1
    imf_year_index = 2
    row_start_index = 3

    tbody: element.Tag = gdp_table.find('tbody')
    rows: element.ResultSet[element.Tag] = tbody.find_all('tr')

    data = {}

    for row in rows[row_start_index:]:
        columns: element.ResultSet[element.Tag] = row.find_all('td')
        country = _strip_footnotes(columns[country_index].text).strip()

        if columns[imf_gdp_index].get('class') and 'table-na' in columns[imf_gdp_index].get('class'):
            gdp = None
            year = None
        else:
            gdp = _strip_footnotes(columns[imf_gdp_index].text).strip()
            year = _strip_footnotes(columns[imf_year_index].text).strip()

        data[country] = {
            'gdp': gdp,
            'year': year
        }

    return data

def extract(url: str, json_path: str) -> None:
    html = _fetch_page(url)
    soup = _parse_html(html)
    gdp_table = _find_gdp_table(soup)
    extracted_data = _get_imf_data(gdp_table)

    with open(json_path, 'w') as file:
        json.dump(extracted_data, file, ensure_ascii=False, indent=2)