import json
import traceback
from typing import List, Dict, Optional

import pandas as pd
import numpy as np

from missions.W1.M3.log.log import Logger

logger = Logger.get_logger()

def numstr2num(numstr: str) -> float:
    if numstr is None:
        return None # Load 시에 transform에서 None으로 가능할 지 생각.
    temp = numstr
    temp = temp.replace(",", "")
    temp = float(temp)
    return temp

def gdpstr_mil2bil(numstr: str) -> float:
    if numstr is None:
        return None # Load 시에 transform에서 None으로 가능할 지 생각.
    temp = numstr
    temp = temp.replace(",", "")
    temp = round(float(temp) / 1000, 2) # 이것을 해도, DB상에서는 float으로 저장되어 Round 처리가 안되어 있음.
    return temp
 
country2region = {
    "China": "East Asia",
    "Japan": "East Asia",
    "South Korea": "East Asia",
    "Taiwan": "East Asia",
    "Hong Kong": "East Asia",
    "Macau": "East Asia",
    "Mongolia": "East Asia",
    "North Korea": "East Asia",
    "Indonesia": "Southeast Asia",
    "Thailand": "Southeast Asia",
    "Vietnam": "Southeast Asia",
    "Malaysia": "Southeast Asia",
    "Philippines": "Southeast Asia",
    "Singapore": "Southeast Asia",
    "Myanmar": "Southeast Asia",
    "Cambodia": "Southeast Asia",
    "Laos": "Southeast Asia",
    "Brunei": "Southeast Asia",
    "East Timor": "Southeast Asia",
    "India": "South Asia",
    "Pakistan": "South Asia",
    "Bangladesh": "South Asia",
    "Sri Lanka": "South Asia",
    "Nepal": "South Asia",
    "Afghanistan": "South Asia",
    "Maldives": "South Asia",
    "Bhutan": "South Asia",
    "Kazakhstan": "Central Asia",
    "Uzbekistan": "Central Asia",
    "Turkmenistan": "Central Asia",
    "Kyrgyzstan": "Central Asia",
    "Tajikistan": "Central Asia",
    "Germany": "Western Europe",
    "United Kingdom": "Western Europe",
    "France": "Western Europe",
    "Italy": "Western Europe",
    "Spain": "Western Europe",
    "Netherlands": "Western Europe",
    "Switzerland": "Western Europe",
    "Belgium": "Western Europe",
    "Sweden": "Western Europe",
    "Ireland": "Western Europe",
    "Austria": "Western Europe",
    "Norway": "Western Europe",
    "Denmark": "Western Europe",
    "Finland": "Western Europe",
    "Portugal": "Western Europe",
    "Greece": "Western Europe",
    "Luxembourg": "Western Europe",
    "Iceland": "Western Europe",
    "Malta": "Western Europe",
    "Monaco": "Western Europe",
    "Andorra": "Western Europe",
    "San Marino": "Western Europe",
    "Liechtenstein": "Western Europe",
    "Russia": "Eastern Europe",
    "Poland": "Eastern Europe",
    "Romania": "Eastern Europe",
    "Czech Republic": "Eastern Europe",
    "Hungary": "Eastern Europe",
    "Ukraine": "Eastern Europe",
    "Slovakia": "Eastern Europe",
    "Bulgaria": "Eastern Europe",
    "Croatia": "Eastern Europe",
    "Lithuania": "Eastern Europe",
    "Slovenia": "Eastern Europe",
    "Latvia": "Eastern Europe",
    "Estonia": "Eastern Europe",
    "Belarus": "Eastern Europe",
    "Moldova": "Eastern Europe",
    "Bosnia and Herzegovina": "Eastern Europe",
    "Albania": "Eastern Europe",
    "North Macedonia": "Eastern Europe",
    "Montenegro": "Eastern Europe",
    "Kosovo": "Eastern Europe",
    "Turkey": "Eastern Europe",
    "Serbia": "Eastern Europe",
    "Azerbaijan": "Eastern Europe",
    "Cyprus": "Eastern Europe",
    "Georgia": "Eastern Europe",
    "Armenia": "Eastern Europe",
    "Saudi Arabia": "Middle East",
    "Iran": "Middle East",
    "United Arab Emirates": "Middle East",
    "Israel": "Middle East",
    "Iraq": "Middle East",
    "Qatar": "Middle East",
    "Kuwait": "Middle East",
    "Oman": "Middle East",
    "Jordan": "Middle East",
    "Lebanon": "Middle East",
    "Bahrain": "Middle East",
    "Syria": "Middle East",
    "Yemen": "Middle East",
    "Palestine": "Middle East",
    "United States": "North America",
    "Canada": "North America",
    "Mexico": "North America",
    "Bermuda": "North America",
    "Cayman Islands": "North America",
    "Aruba": "North America",
    "Curaçao": "North America",
    "Greenland": "North America",
    "Sint Maarten": "North America",
    "Turks and Caicos Islands": "North America",
    "Brazil": "Latin America",
    "Argentina": "Latin America",
    "Colombia": "Latin America",
    "Chile": "Latin America",
    "Peru": "Latin America",
    "Venezuela": "Latin America",
    "Ecuador": "Latin America",
    "Bolivia": "Latin America",
    "Paraguay": "Latin America",
    "Uruguay": "Latin America",
    "Cuba": "Latin America",
    "Dominican Republic": "Latin America",
    "Puerto Rico": "Latin America",
    "Panama": "Latin America",
    "Costa Rica": "Latin America",
    "Guatemala": "Latin America",
    "Honduras": "Latin America",
    "El Salvador": "Latin America",
    "Nicaragua": "Latin America",
    "Jamaica": "Latin America",
    "Trinidad and Tobago": "Latin America",
    "Bahamas": "Latin America",
    "Barbados": "Latin America",
    "Guyana": "Latin America",
    "Suriname": "Latin America",
    "Belize": "Latin America",
    "Haiti": "Latin America",
    "Saint Lucia": "Latin America",
    "Grenada": "Latin America",
    "Saint Kitts and Nevis": "Latin America",
    "Saint Vincent and the Grenadines": "Latin America",
    "Antigua and Barbuda": "Latin America",
    "Dominica": "Latin America",
    "Australia": "Oceania",
    "New Zealand": "Oceania",
    "Papua New Guinea": "Oceania",
    "Fiji": "Oceania",
    "Solomon Islands": "Oceania",
    "Vanuatu": "Oceania",
    "New Caledonia": "Oceania",
    "French Polynesia": "Oceania",
    "Samoa": "Oceania",
    "Tonga": "Oceania",
    "Micronesia": "Oceania",
    "Kiribati": "Oceania",
    "Palau": "Oceania",
    "Marshall Islands": "Oceania",
    "Nauru": "Oceania",
    "Tuvalu": "Oceania",
    "Egypt": "North Africa",
    "Algeria": "North Africa",
    "Morocco": "North Africa",
    "Tunisia": "North Africa",
    "Libya": "North Africa",
    "Nigeria": "Sub-Saharan Africa",
    "South Africa": "Sub-Saharan Africa",
    "Ethiopia": "Sub-Saharan Africa",
    "Kenya": "Sub-Saharan Africa",
    "Ghana": "Sub-Saharan Africa",
    "Angola": "Sub-Saharan Africa",
    "Tanzania": "Sub-Saharan Africa",
    "DR Congo": "Sub-Saharan Africa",
    "Sudan": "Sub-Saharan Africa",
    "Uganda": "Sub-Saharan Africa",
    "Cameroon": "Sub-Saharan Africa",
    "Ivory Coast": "Sub-Saharan Africa",
    "Madagascar": "Sub-Saharan Africa",
    "Zambia": "Sub-Saharan Africa",
    "Senegal": "Sub-Saharan Africa",
    "Zimbabwe": "Sub-Saharan Africa",
    "Guinea": "Sub-Saharan Africa",
    "Rwanda": "Sub-Saharan Africa",
    "Benin": "Sub-Saharan Africa",
    "Burkina Faso": "Sub-Saharan Africa",
    "Somalia": "Sub-Saharan Africa",
    "Mali": "Sub-Saharan Africa",
    "Malawi": "Sub-Saharan Africa",
    "Mauritius": "Sub-Saharan Africa",
    "Togo": "Sub-Saharan Africa",
    "Eswatini": "Sub-Saharan Africa",
    "Sierra Leone": "Sub-Saharan Africa",
    "Namibia": "Sub-Saharan Africa",
    "Botswana": "Sub-Saharan Africa",
    "Lesotho": "Sub-Saharan Africa",
    "Gambia": "Sub-Saharan Africa",
    "Gabon": "Sub-Saharan Africa",
    "Mauritania": "Sub-Saharan Africa",
    "Guinea-Bissau": "Sub-Saharan Africa",
    "Equatorial Guinea": "Sub-Saharan Africa",
    "Djibouti": "Sub-Saharan Africa",
    "Comoros": "Sub-Saharan Africa",
    "Cape Verde": "Sub-Saharan Africa",
    "São Tomé and Príncipe": "Sub-Saharan Africa",
    "Seychelles": "Sub-Saharan Africa",
    "Eritrea": "Sub-Saharan Africa",
    "South Sudan": "Sub-Saharan Africa",
    "Burundi": "Sub-Saharan Africa",
    "Liberia": "Sub-Saharan Africa",
    "Central African Republic": "Sub-Saharan Africa",
    "Chad": "Sub-Saharan Africa",
    "Niger": "Sub-Saharan Africa",
    "Mozambique": "Sub-Saharan Africa",
    "Congo": "Sub-Saharan Africa",
    "Zanzibar": "Sub-Saharan Africa"
}

def transform_gdp_data(data: List[Dict]) -> Optional[List[Dict]]:
    """
    Transform the data.

    Return the transformed data.
    1. 'gdp' column is converted to float and divided by 1000 and rounded to 2 decimal places.
    2. 'year' column is converted to int.
    3. 'region' column is added based on the 'country' column. If the country is not in the country2region dictionary, 'Unknown' is added.

    """
    try:
        logger.info("데이터 변환 시도")
        transformed_data = []
        recent_row = None
        for row in data:
            recent_row = row
            transformed_data.append(
                {
                    'Country': row['country'],
                    'GDP_USD_billion': gdpstr_mil2bil(row['gdp']),
                    'Year': int(row['year']) if row['year'] else None,
                    'Type': row['type'],
                    'Region': country2region.get(row['country'], "Unknown")
                }
            )
        return transformed_data
       
    except Exception as e:
        full_err_msg = traceback.format_exc(chain=True)
        err_msg = full_err_msg.split('\n')[-2]
        logger.info(f'데이터 변환 중 에러 발생: {e} / recent_row: {recent_row}')
        logger.info(f'Full message: {full_err_msg}')
        logger.info(f'Short message: {err_msg}')
        return None

def transform_gdp_df(gdp_df: pd.DataFrame) -> Optional[List[Dict]]:
    """
    Transform the data.

    Return the transformed df.
    1. 'gdp' column (USD_Million) is converted to float and divided by 1000 (USD_Billion)and rounded to 2 decimal places.
    2. 'year' column is converted to int.
    3. 'region' column is added based on the 'country' column. If the country is not in the country2region dictionary, 'Unknown' is added.

    """
    try:
        logger.info("데이터 변환 시도")
    
        # Column rename
        gdp_df.rename(columns={
            'country': 'Country', 
            'gdp_usd_million': 'GDP_USD_Million', 
            'year': 'Year',
            'type': 'Type',
            }, inplace=True)        
        
        # Convert GDP_USD_Million to GDP_USD_Billion
        gdp_billion_series = pd.to_numeric(
            np.char.replace(gdp_df['GDP_USD_Million'].values.astype(str), ',', '')
            , errors='coerce'   # ignore를 사용하는 것도 좋아보임. None으로 처리됨.
            ) / 1000
        gdp_df["GDP_USD_Million"] = gdp_billion_series.round(2)
        gdp_df.rename(columns={'GDP_USD_Million': 'GDP_USD_Billion'}, inplace=True)

        # Convert Year to Int16
        gdp_df["Year"] = gdp_df["Year"].astype("Int16")

        # Make country2region df
        country2region_df = pd.DataFrame(
            list(country2region.items()), 
            columns=['Country', 'Region']
            )
        
        # Merge
        gdp_df = pd.merge(gdp_df, country2region_df, on='Country', how='left')
        # 없는 경우는 NaN으로 처리됨.
        gdp_df["Region"] = gdp_df["Region"].fillna("Unknown")

        return gdp_df
       
    except Exception as e:
        full_err_msg = traceback.format_exc(chain=True)
        err_msg = full_err_msg.split('\n')[-2]
        logger.info(f'데이터 변환 중 에러 발생: {e}')
        logger.info(f'Full message: {full_err_msg}')
        logger.info(f'Short message: {err_msg}')
        return None
    
def load_gdp_json(json_path: str) -> Optional[List[Dict]]:
    """
    JSON file load.

    Return the loaded data.
    """
    try:
        logger.info("GDP JSON 로드 시도")
        with open(json_path, 'r') as json_file:
            gdp_data = json.load(json_file)
        return gdp_data
    
    except Exception as e:
        full_err_msg = traceback.format_exc(chain=True)
        err_msg = full_err_msg.split('\n')[-2]
        logger.info(f'GDP JSON 로드 중 에러 발생: {e}')
        logger.info(f'Full message: {full_err_msg}')
        logger.info(f'Short message: {err_msg}')
        return None

def load_df_from_json(json_path: str) -> Optional[pd.DataFrame]:
    """
    JSON file load.

    Return the dataframe.
    """
    try:
        logger.info("GDP JSON 로드 시도")
        df = pd.read_json(json_path, orient='records', encoding='utf-8')
        return df
    
    except Exception as e:
        full_err_msg = traceback.format_exc(chain=True)
        err_msg = full_err_msg.split('\n')[-2]
        logger.info(f'GDP JSON 로드 중 에러 발생: {e}')
        logger.info(f'Full message: {full_err_msg}')
        logger.info(f'Short message: {err_msg}')
        return None
            

if __name__ == "__main__":
    print(numstr2num("2,100"))