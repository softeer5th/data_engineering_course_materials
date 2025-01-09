# 표준 라이브러리
import json
# 서드파티 라이브러리
import requests
from bs4 import BeautifulSoup

# 로컬 모듈
from missions.W1.M3.log.log import Logger

logger = Logger.get_logger("GDP_ETL_LOGGER")

###########
#  1. Fetch
###########
def _fetch_web(url: str)-> str:
    """
    Fetches the content of a webpage from the specified URL.

    Args:
        url (str): The URL of the webpage to fetch.

    Returns:
        str: The HTML content of the fetched webpage as a string.

    Raises:
        HTTPError: If the HTTP request fails or the response status code is not 200.
    """ 
    try:
        logger.info(f"[START] Attempting to fetch the webpage: {url}")
        response = requests.get(url)
        response.raise_for_status()
        logger.info(f"[COMPLETE] Successfully fetched the webpage: {url}")
    except requests.exceptions.HTTPError as e:
        logger.error(f"[FAIL] Failed to fetch the webpage: {url}. Status Code: {response.status_code}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"[FAIL] Failed to fetch the webpage: {url}. Exception: {e.__class__.__name__}")
        raise
    return response.text

###########
#  2. Parse
###########
def _decompose_all_sup(components: any)-> any:
    """
    Decomposes all 'sup' tags within the given components.

    Args:
        components (any): A list of components to search through. Each component is expected to support the 
                          'find_all' method to locate 'sup' tags.

    Returns:
        any: The components with all 'sup' tags removed.
    """

    for component in components:
        for sup in component.find_all("sup"):
            sup.decompose()
    return components

def _parse_imf_gdp_data(tr)-> dict:
    """
    Parses a table row to extract GDP data.

    Args:
        tr (any): A BeautifulSoup object representing a table row.

    Returns:
        dict or None: A dictionary containing the following keys:
            - 'country' (str): The name of the country.
            - 'gdp' (str or None): The GDP value, or None if marked unavailable.
            - 'year' (str or None): The year of the GDP data, or None if marked unavailable.
            - 'type' (str): A constant string 'IMF' indicating the data source.
            
        Returns None if the row is identified as a header ('static-row-header').
    """

    if 'static-row-header' in tr.get('class', []):
        return None
    
    tds = tr.find_all(["td"])
    tds = _decompose_all_sup(tds)

    isnull = False
    if "table-na" in tds[1].get("class", []):
        isnull = True
    return {
        'country': tds[0].get_text(strip=True),
        'gdp': tds[1].get_text(strip=True) if not isnull else None,
        'year': tds[2].get_text(strip=True) if not isnull else None,
        'type': 'IMF'
        }

def _parse_all_imf_gdp_data(gdp_table)-> any:
    """
    Extracts GDP data from a given table.
    Returns the list of extracted data.

    Args:
        gdp_table (any): A BeautifulSoup object representing the GDP table 
                         from which data is to be extracted.

    Returns:
        list: A list containing extracted GDP data.
    """

    all_data = []
    for tr in gdp_table.find_all("tr"):
        try:
            data = _parse_imf_gdp_data(tr)
            if data:
                all_data.append(data)
        except:
            logger.warn(f"[FAIL] A GDP data extraction failed. data: {data}")
            raise
    return all_data

def _parse_gdp_table(wiki_tables: any, caption: str = "GDP (million US$) by country")-> any:
    """
    Searches for a specific table in a list of 'wikitable' tables by its caption.

    Args:
        wiki_tables (any): A list of 'wikitable' tables to search through.
        caption (str, optional): The caption text to identify the GDP table. Defaults to "GDP (million US$) by country".

    Returns:
        any: The table matching the specified caption, or None if no matching table is found.
    """
    
    for table in wiki_tables:
        caption = table.find("caption")
        if caption and "GDP (million US$) by country" in caption.get_text():
            return table
    
    logger.warn("[FAIL] No matching GDP table found.")
    return None
    
def _parse_wiki_tables(soup: BeautifulSoup)-> any:
    """
    Extracts all tables with the class 'wikitable' in a HTML using BeautifulSoup.

    Args:
        soup (BeautifulSoup): A BeautifulSoup object representing the parsed HTML content.

    Returns:
        any: A list of tables with the class 'wikitable' if found, or an empty list if none are found.
    """

    wiki_tables = soup.find_all("table", {"class": "wikitable"})
    if wiki_tables is None:
        logger.warn("[FAIL] No matching any wiki table found")
    return wiki_tables

def _parse_gdp_wikipage(url, raw_html)-> dict:
    logger.info(f"[START] Starting to parse the webpage: {url} ...")

    soup = BeautifulSoup(raw_html, "html.parser")
    wiki_tables = _parse_wiki_tables(soup)
    gdp_table = _parse_gdp_table(wiki_tables)
    all_imf_gdp_data = _parse_all_imf_gdp_data(gdp_table)

    logger.info(f"[COMPLETE] Succesfuly finished parsing the webpage: {url}")
    return all_imf_gdp_data

###########
#  3. Save to JSON
###########
def _save2json(data: dict, out_file: str)-> None:
    try:
        logger.info(f"[START] Saving data to {out_file} ...")
        with open(out_file, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, indent=4, ensure_ascii=False)
        logger.info(f"[COMPLETE] Data successfully saved to {out_file}.")
    except (OSError, json.JSONDecodeError) as e:
        logger.error(f"[FAIL] Failed to save data to {out_file}. Exception: {e.__class__.__name__}")
        raise


###########
#  Extract Process
###########
def extract_gdp_wiki_web2json(url: str, out_file: str) -> None:
    """
    Extracts GDP data from a Wikipedia webpage and saves it to a JSON file.

    Args:
        url (str): The URL of the Wikipedia page containing GDP data.
        out_file (str): The path to the output JSON file where the extracted data will be saved.

    Returns:
        None: This function does not return a value. The extracted data is saved to the specified JSON file.
    """
    try:
        logger.info("[START] Starting GDP extraction process...")

        raw_html = _fetch_web(url)
        gdp_data = _parse_gdp_wikipage(url, raw_html)
        _save2json(gdp_data, out_file)
        
        logger.info("[COMPLETE] GDP extraction process completed successfully.")
    except:
        logger.warn("[FAIL] GDP extraction process failed.")
        raise
    

def main():
    wiki_url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
    out_file = "missions/W1/M3/data/Countries_by_GDP.json"

    extract_gdp_wiki_web2json(wiki_url, out_file)

if __name__ == "__main__":
    main()