import requests
import datetime as dt
import json
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3


# Name: Extract
# Description: Handles extraction of raw data from a URL and saves it locally
class Extract:
    def __init__(self, url, path):
        self.url = url
        self.path = path
        self.response = None
        self.raw_data = None

    # Name: run
    # Parameter: self
    # Return: dict raw_data
    def run(self):
        writeLog("Extract Start", PATH_LOG)
        self.response = self.get_response_from_url(self.url)
        self.raw_data = self.save_raw_data(self.response, self.path, self.url)
        writeLog("Extract Finished", PATH_LOG)
        return self.raw_data

    # Name: get_response_from_url
    # Parameter: url (str)
    # Return: Response object or None
    @staticmethod
    def get_response_from_url(url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            print(f"Error: request: {e}")
            return None

    # Name: save_raw_data
    # Parameter: response (Response), path (str), url (str)
    # Return: dict raw_data or None
    @staticmethod
    def save_raw_data(response, path, url):
        if response is None:
            print("Error: response is None")
            return None

        now = dt.datetime.now().strftime("%Y-%b-%d-%H-%M-%S")
        raw_data = {"url": url, "text": response.text, "date": now}

        try:
            with open(path, "w") as f:
                json.dump(raw_data, f)
            return raw_data
        except Exception as e:
            print(f"Error: raw data: {e}")
            return None


# Name: Transform
# Description: Processes raw data and transforms it into a structured DataFrame
class Transform:
    def __init__(self, raw_data):
        self.raw_data = raw_data
        self.table = None
        self.soup = None
        self.df = None

    def open_json(raw_data):
        try:
            js = json.load(raw_data["text"])
            return js
        except:
            return False

    # Name: get_soup
    # Parameter: raw_data (dict)
    # Return: BeautifulSoup object
    @staticmethod
    def get_soup(raw_data):
        return BeautifulSoup(raw_data["text"], "lxml")

    # Name: get_table
    # Parameter: soup (BeautifulSoup), css_selector_type (str), css_selector (str)
    # Return: BeautifulSoup Table object or None
    @staticmethod
    def get_table(soup, css_selector_type, css_selector):
        type_dict = {"class": ".", "ID": "#", "tag_name": ""}  # css 선택자 타입

        # 유효성 검사
        if css_selector_type not in type_dict:
            print("Error: css_selector_type: Wrong css_selector_type")
            return None

        # 클래스인 경우 공백문자 .으로 치환
        if css_selector_type == "class":
            css_selector = css_selector.replace(" ", ".")

        return soup.select_one(type_dict[css_selector_type] + css_selector)

    # Name: get_dataframe
    # Parameter: table (BeautifulSoup Table)
    # Return: DataFrame
    @staticmethod
    def get_dataframe(table):
        if type(table) == dict:
            return pd.read_json(table)
        return pd.read_html(str(table))[0] if table else None

    @staticmethod
    def unarchive_dict(dic, n):
        for i in range(n):
            dic = list(dic.values())[0]
        return dic

    # Name: filter_dataframe
    # Parameter: df (DataFrame), kwargs (dict)
    # Return: DataFrame
    @staticmethod
    def filter_dataframe(df, **kwargs):

        # 멀티인덱스 드랍
        if "droplevel" in kwargs:
            df.columns = df.columns.droplevel(kwargs["droplevel"])

        # 사용할 열
        if "column_nums" in kwargs and kwargs["column_nums"]:
            df = df.iloc[:, kwargs["column_nums"]]

        # NaN 드랍 여부
        if kwargs.get("row_dropna", False):
            df.dropna(axis=0, inplace=True)

        # 하이픈 드랍 여부
        if kwargs.get("row_drophyphen", False):
            df.replace("—", pd.NA, inplace=True)
            df.dropna(axis=0, inplace=True)

        # 컬럼 이름
        if "column_names" in kwargs and kwargs["column_names"]:
            df.columns = kwargs["column_names"]

        # 타입 세팅
        if "astype" in kwargs and kwargs["astype"]:
            df = df.astype(kwargs["astype"])

        # 인덱스 초기화 여부
        if kwargs.get("reset_index", False):
            df = df.reset_index(drop=True)

        return df

    # Name: run
    # Parameter: self, css_selector_type (str), css_selector (str), **filter_args
    # Return: DataFrame
    def run_html(self, css_selector_type, css_selector, **filter_args):
        self.soup = self.get_soup(self.raw_data)
        self.table = self.get_table(self.soup, css_selector_type, css_selector)
        self.df = self.get_dataframe(self.table)
        if self.df is not None:
            self.df = self.filter_dataframe(self.df, **filter_args)
        return self.df

    # Name: run
    # Parameter self, js, unarchive, **filter_args
    # Return: DataFrame
    def run_js(self, js, unarchive, **filter_args):
        js = self.open_json(js)
        js = self.unarchive_dict(unarchive)
        self.df = self.get_dataframe(js)
        if self.df is not None:
            self.df = self.filter_dataframe(self.df, **filter_args)
        return self.df


# Name: TransformGDP
# Description: Extends Transform for GDP-specific transformations
class TransformGDP(Transform):
    def __init__(self, raw_data, path_region):
        super().__init__(raw_data)
        self.path_region = path_region

    # Name: bil_to_mil
    # Parameter: self
    # Return: DataFrame
    def bil_to_mil(self):
        self.df["GDP_USD_billion"] = (self.df["GDP_USD_billion"] / 1000).round(2)
        return self.df

    # Name: add_region_column
    # Parameter: self, df (DataFrame), path_region (str)
    # Return: DataFrame
    def add_region_column(self, df, path_region):
        region_df = pd.read_csv(path_region)
        region_df = pd.DataFrame(
            {"Country": region_df["name"], "Region": region_df["region"]}
        )
        self.df = pd.merge(df, region_df, how="left", on="Country").dropna()
        return self.df

    # Name: run_html
    # Parameter: self, path_region (str), css_selector_type (str), css_selector (str), **filter_args
    # Return: DataFrame
    def run_html(self, path_region, css_selector_type, css_selector, **filter_args):
        writeLog("Transform Start", PATH_LOG)
        self.df = super().run_html(css_selector_type, css_selector, **filter_args)
        self.bil_to_mil()
        self.add_region_column(self.df, path_region)
        writeLog("Transform Finished", PATH_LOG)
        return self.df

    # Name: run
    # Parameter self, js, unarchive, **filter_args
    # Return: DataFrame
    def run_js(self, path_region, js, unarchive, **filter_args):
        writeLog("Transform Start", PATH_LOG)
        self.df = super().run_js(js, unarchive, **filter_args)
        self.bil_to_mil()
        self.add_region_column(self.df, path_region)
        writeLog("Transform Finished", PATH_LOG)

        return self.df


# Load
class Load:
    def __init__(self, df, path_db):
        self.df = df
        self.path_db = path_db
        self.con = None
        self.cursor = None
        self.sql = None

    # Name: open_con
    # Parameter: Path
    # Return: cursor
    def open_con(self, path_db=None):
        if path_db:
            self.path_db = path_db
        self.con = sqlite3.connect(self.path_db)
        self.cursor = self.con.cursor()
        return self.cursor

    # Name: create_table
    # Parameter: df_name(Str), df(DataFrame), index(Bool)
    # Return: df(DataFrame)
    def create_table(self, df_name, df=None, index=None):
        if df:
            self.df = df

        if index == False:
            self.df.to_sql(
                name=df_name,
                con=self.con,
                if_exists="replace",
                index=False,
            )
        else:
            self.df.to_sql(
                name=df_name,
                con=self.con,
                if_exists="replace",
            )
        self.con.commit()
        return self.df

    # Name: close_con
    # Parameter: con
    # Return: True or Error
    def close_con(self, con=None):
        try:
            if con:
                self.con = con
                self.con.close()
                return True
        except Exception as e:
            return e

    # Name: execute_sql()
    # Parameter: sql(Str)
    # Return: rows
    def execute_sql(self, sql):
        if sql:
            self.sql = sql
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()

        return rows


class LoadGDP(Load):
    def __init__(
        self,
        df,
        path_db,
    ):
        super().__init__(df, path_db)

    # Name: print_GDP_over_nB
    # Parameter: n
    # Return: rows
    def print_GDP_over_nB(self, n):
        print(f"\nGDP가 {n}B 이상인 국가들")
        sql = """
        SELECT Country, GDP_USD_billion
        FROM Countries_by_GDP
        WHERE GDP_USD_billion > """ + str(
            n
        )
        rows = LoadGDP.execute_sql(self, sql)
        print("Country              | GDP_USD_billion")
        for row in rows:
            country = "%-20s" % row[0]
            GDP = row[1]
            print(country + " |", GDP)

        return rows

    # Name: print_avg_Top_N_GDP_by_region
    # Parameter: n
    # Return: df
    def print_avg_Top_N_GDP_by_region(self, n):
        print("\n지역별 탑5 국가의 GDP 평균")
        regions = ["Africa", "Americas", "Asia", "Europe", "Oceania"]
        data = []
        print("Region   | Avg of Top 5")
        for region in regions:
            sql = (
                '''
            SELECT AVG(GDP_USD_billion)
            FROM (
            SELECT GDP_USD_billion
            FROM Countries_by_GDP
            WHERE Region = "'''
                + region
                + """"
            ORDER BY GDP_USD_billion DESC
            LIMIT 5
            )
            """
            )
            form = [region, LoadGDP.execute_sql(self, sql)[0][0]]
            data.append(form)
            print("%-10s" % form[0], round(form[1], 2))

        return pd.DataFrame(data)


# GDP 100B 이상인 국가 데이터프레임
def get_GDP_over_100B(df):
    df = df[df.GDP_USD_billion > 100]
    return df


# 지역별 GDP 탑 5 국가의 GDP 평균
def get_top_n_avg_gdp_by_region(df, top_n=5):
    print("지역별 탑 5의 GDP 평균")
    sorted_df = df.sort_values(
        by=["Region", "GDP_USD_billion"], ascending=[True, False]
    )
    top_GDP = sorted_df.groupby("Region").head(top_n)
    avg_GDP = top_GDP.groupby("Region")["GDP_USD_billion"].mean().reset_index()
    avg_GDP.sort_values(by=["GDP_USD_billion"], ascending=False, inplace=True)
    return avg_GDP


# writeLog
def writeLog(log, path_log):
    now = dt.datetime.now()
    log = now.strftime("%Y-%b-%d-%H-%M-%S, ") + log + "\n"
    with open(path_log, "a") as f:
        f.write(log)


# const block
URL = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
CSS_SELECTOR_TYPE = "class"
CSS_SELECTOR = "wikitable sortable sticky-header-multi static-row-numbers"
PATH_RAW_DATA = "missions/W1/M3/data/Countries_by_GDP.json"
PATH_REGION = "missions/W1/M3/data/region.csv"
PATH_LOG = "missions/W1/M3/data/etl_project_log.txt"
PATH_DB = "missions/W1/M3/data/World_Economies.db"
TABLE_NAME = "Countries_by_GDP"

# URL = 'https://www.imf.org/external/datamapper/api/v1/NGDPD'


# main

# Extract
e = Extract(URL, PATH_RAW_DATA)
raw_data = e.run()

# Transform
t = TransformGDP(raw_data, PATH_REGION)
df = t.run_html(
    PATH_REGION,
    CSS_SELECTOR_TYPE,
    CSS_SELECTOR,
    droplevel=0,
    column_nums=[0, 1],
    row_dropna=True,
    row_drophyphen=True,
    column_names=["Country", "GDP_USD_billion"],
    astype={"GDP_USD_billion": "float"},
    reset_index=True,
)

# Transform(js)
# df = t.run_js(PATH_REGION, raw_data, 2,
#             droplevel = None,
#             column_nums = [0, 1],
#             row_dropna= True,
#             row_drophyphen = True,
#             column_names=['Country', 'GDP_USD_billion'],
#             astype={'GDP_USD_billion' : 'float'},
#             reset_index = True)

# Load
l = LoadGDP(df, PATH_DB)
writeLog("Load Start", PATH_LOG)
l.open_con(PATH_DB)
l.create_table(TABLE_NAME)
l.close_con()
writeLog("Load finished", PATH_LOG)

l.open_con(PATH_DB)
l.print_GDP_over_nB(100)
l.print_avg_Top_N_GDP_by_region(5)
l.close_con()

# Result
# print(get_GDP_over_100B(df))
# print(get_top_n_avg_gdp_by_region(df))
