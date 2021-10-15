from typing import Tuple, Dict
import os
import yaml as yl
import json
import pandas as pd

from datetime import datetime, timedelta

from handlers.db_handler import get_db_handler, PostgresHandler
from handlers.in_memory_handler import get_in_memory_handler, RedisHandler
from config.controllers import ColumnController
from constants import DATE_FORMAT


class SMACross:
    def __init__(
        self, ticker: str, sma_db: PostgresHandler, cross_db: RedisHandler
    ) -> None:

        self.ticker = ticker
        self.sma_dbj = sma_db
        self.cross_db = cross_db

    def _get_cross_info(self) -> Dict:
        return json.loads(self.cross_db.get(ticker))

    def _get_sma_info(self) -> Dict:
        data = self.sma_db.get_data(ticker, condition="ORDER BY DATE DESC LIMIT 1")
        return data.to_dict("records")[0]

    def check_sma_cross(self) -> Tuple[datetime.date, str, str]:

        buffer = 1.0
        date = self.slice["date"].iloc[0]
        cross_info = self._get_cross_info()
        sma_info = self._get_sma_info()

        cross_info["last_cross_up"] = datetime.strptime(
            cross_info["last_cross_up"], DATE_FORMAT
        )
        cross_info["last_cross_down"] = datetime.strptime(
            cross_info["last_cross_down"], DATE_FORMAT
        )

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
    DATE_FORMAT = "%Y-%m-%d"
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
    sma_db = get_db_handler(DB_HANDLER, [ticker], DB_INFO)
    in_mem = get_in_memory_handler(IN_MEM_HANDLER, IN_MEMORY_INFO)

    # Define a start pull date to run the simulation
    start_date = "2021-07-11"
    curr_date = datetime.strptime(start_date, DATE_FORMAT)
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
