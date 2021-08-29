from typing import Dict
import os
import sys
import yaml as yl
import pandas as pd
import sqlalchemy as sa
from db_handler import *
from dotenv import load_dotenv
from controllers import ColumnController


load_dotenv("../local.env")


class SMACross:

    def __init__(self, ticker: str, db_info: Dict) -> None:
        self.ticker = ticker
        self.db_info = db_info

        self.db_engine = None

        self._initialize_db_connection()


    def _initialize_db_connection(self) -> None:
        user = self.db_info.get("user")
        password = self.db_info.get("password")
        database = self.db_info.get("database")
        host = self.db_info.get("host")
        port = self.db_info.get("port", "5432")

        self.db_engine = sa.create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        )

    def _get_table_slice(self):
        condition = "ORDER BY date DESC LIMIT 6"
        query = f"SELECT * FROM {self.ticker} {condition}".strip()
        df = pd.read_sql(query, con=self.db_engine)
        return df


    def check_sma_cross(self):
        df_slice = self._get_table_slice()

        #buffer up should be greater than 1 and buffer down should be lower than 1 if you want to make breakout signals stronger
        buffer_up = 1
        buffer_down = 1

    # TODO: Need to figure out if the dataframe that I am analyzing is going to be in ascending or descending order. Currently it is in descending order when analyzing.
        if df_slice['close'].iloc[0] <= df_slice ['ma_50'].iloc[0] or df_slice['ma_50'].iloc[0] <= df_slice['ma_200'].iloc[0]:
            return 'BEAR MARKET'
        else:
            if (buffer_up * (df_slice['ma_7'].iloc[0]) >= df_slice['ma_21'].iloc[0]) and (df_slice['ma_7'].iloc[1] < df_slice['ma_21'].iloc[1]):
                return (df_slice['date'].iloc[0],self.ticker, 'BUY')

            elif (buffer_down * (df_slice['ma_7'].iloc[0]) < df_slice['ma_21'].iloc[0]) and (df_slice['ma_7'].iloc[1] >= df_slice['ma_21'].iloc[1]):
                return (df_slice['date'].iloc[0],self.ticker, 'SELL')
            else:
                return "HOLD"


if __name__ == "__main__":
    # Loading in DB info
    HOST = os.getenv("POSTGRES_HOST")
    DATABASE = os.getenv("POSTGRES_DB")
    USER = os.getenv("POSTGRES_USER")
    PASSWORD = os.getenv("POSTGRES_PASSWORD")

    # Loading in and parsing CONFIG
    CONFIG = yl.safe_load(open("config.yml", "r"))
    TICKERS = CONFIG["ticker_list"]
    DB_HANDLER = CONFIG["db_handler"]
    DATA_HANDLER = CONFIG["data_handler"]

    # Building global vars for processing
    DATE_FORMAT = "%Y-%m-%d"
    DB_INFO = {
        "host": HOST,
        "database": DATABASE,
        "user": USER,
        "password": PASSWORD,
        "port": "5432",
    }

    #create aapl slice
    aapl_class = SMACross('aapl', DB_INFO)
    print(aapl_class.check_sma_cross())