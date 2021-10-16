from typing import Tuple, Dict
import os
import yaml as yl
import json
import pandas as pd

from datetime import datetime, timedelta

from algo_trading.handlers.db_handler import DBRepository
from algo_trading.handlers.in_memory_handler import get_in_memory_handler, RedisHandler
from algo_trading.utils.utils import str_to_dt

"""Description of algorithm:


"""


class SMACross:
    def __init__(
        self, ticker: str, sma_db: DBRepository, cross_db: RedisHandler
    ) -> None:

        self.ticker = ticker
        self.sma_db = sma_db
        self.cross_db = cross_db

    @property
    def cross_info(self) -> Dict:
        return json.loads(self.cross_db.get(ticker))

    @property
    def sma_info(self) -> Dict:
        data = self.sma_db.get_days_back(ticker, 1)
        return data.to_dict("records")[0]

    def check_sma_cross(self) -> Tuple[datetime.date, str, str]:

        buffer = 1.0
        date = self.slice["date"].iloc[0]

        last_cross_up_dt = str_to_dt(self.cross_info["last_cross_up"])
        last_cross_down_dt = str_to_dt(self.cross_info["last_cross_down"])

        # Steps
        # Which date in


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv("../local.env")

    # Loading in DB info
    HOST = os.getenv("POSTGRES_HOST")
    DATABASE = os.getenv("POSTGRES_DB")
    USER = os.getenv("POSTGRES_USER")
    PASSWORD = os.getenv("POSTGRES_PASSWORD")

    # Loading in IN MEMORY info
    IN_MEM_HOST = os.getenv("REDIS_HOST")
    IN_MEM_PORT = os.getenv("REDIS_PORT")
    IN_MEM_DATABASE = os.getenv("REDIS_DB")
    IN_MEM_PASSWORD = os.getenv("REDIS_PASSWORD")

    # Loading in and parsing CONFIG
    CONFIG = yl.safe_load(open("config.yml", "r"))
    DB_HANDLER = CONFIG["db_handler"]
    DATA_HANDLER = CONFIG["data_handler"]
    IN_MEM_HANDLER = CONFIG["in_memory_handler"]

    # Building global vars for processing
    DB_INFO = {
        "host": HOST,
        "database": DATABASE,
        "user": USER,
        "password": PASSWORD,
        "port": "5432",
    }

    IN_MEMORY_INFO = {
        "host": IN_MEM_HOST,
        "port": IN_MEM_PORT,
        "db": IN_MEM_DATABASE,
        "password": IN_MEM_PASSWORD,
    }

    # Creating db_handler (works with just one stock for now)
    ticker = "aapl"
    sma_db = DBRepository([ticker], DB_INFO, DB_HANDLER)
    in_mem = get_in_memory_handler(IN_MEM_HANDLER, IN_MEMORY_INFO)

    # Define a start pull date to run the simulation
    start_date = "2021-07-11"
    curr_date = str_to_dt(start_date)
    last_date_entry = sma_db.get_most_recent_date(ticker)

    # Running SMA for every day since the given start date up until the most
    # recent date in the DB
    while curr_date <= datetime(
        last_date_entry.year, last_date_entry.month, last_date_entry.day
    ):

        sma = SMACross(ticker, sma_db, in_mem)
        result = sma.check_sma_cross()
        print(result)
        curr_date = curr_date + timedelta(days=1)
