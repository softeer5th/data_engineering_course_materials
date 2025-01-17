import pandas as pd
import sqlite3
from lib import log, constant

STATE = "LOAD"

def load_to_db(gdp_imf: pd.DataFrame):
    log_full_name = constant.PATH + constant.LOG_NAME
    db_full_name = constant.PATH + constant.DB_NAME
    log.write_log(log_full_name, STATE, is_start=True)
    try:
        con = sqlite3.connect(db_full_name)
        gdp_imf.to_sql('Countries_by_GDP',con, if_exists='replace')
        con.close()
    except Exception as e:
        log.write_log(log_full_name, STATE, is_error=True, msg=e)
        exit(1)
    log.write_log(log_full_name, STATE, is_start=False)
    