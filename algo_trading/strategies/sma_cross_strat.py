from typing import Tuple, Dict
import os
import yaml as yl
import json
import pandas as pd

from datetime import datetime, timedelta

from algo_trading.repositories.db_repository import DBRepository
from algo_trading.repositories.key_val_repository import KeyValueRepository
from algo_trading.utils.utils import str_to_dt

"""Description of algorithm:


"""


class SMACross:
    def __init__(
        self,
        ticker: str,
        sma_db: DBRepository,
        cross_db: KeyValueRepository,
    ) -> None:

        self.ticker = ticker
        self.sma_db = sma_db
        self.cross_db = cross_db.handler

    @property
    def cross_info(self) -> Dict:
        return json.loads(self.cross_db.get(ticker))

    @property
    def sma_info(self) -> Dict:
        data = self.sma_db.get_days_back(ticker, 1)
        return data.to_dict("records")[0]

    def check_sma_cross(self) -> Tuple[datetime.date, str, str]:

        buffer = 1.0

        # last_cross_up_dt = str_to_dt(self.cross_info["last_cross_up"])
        # last_cross_down_dt = str_to_dt(self.cross_info["last_cross_down"])

        print("doing sma...")
        # Steps
        # Which date in


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv("../../local.env")

    # Loading in DB info
    HOST = os.getenv("POSTGRES_HOST")
    DATABASE = os.getenv("POSTGRES_DB")
    USER = os.getenv("POSTGRES_USER")
    PASSWORD = os.getenv("POSTGRES_PASSWORD")

    # Loading in IN MEMORY info
    KV_HOST = os.getenv("REDIS_HOST")
    KV_PORT = os.getenv("REDIS_PORT")
    KV_DATABASE = os.getenv("REDIS_DB")
    KV_PASSWORD = os.getenv("REDIS_PASSWORD")

    # Loading in and parsing CONFIG
    CONFIG = yl.safe_load(open("../config/config.yml", "r"))
    DB_HANDLER = CONFIG["db_repo"]
    DATA_HANDLER = CONFIG["data_repo"]
    KV_HANDLER = CONFIG["kv_repo"]

    # Building global vars for processing
    DB_INFO = {
        "host": HOST,
        "db_name": DATABASE,
        "user": USER,
        "password": PASSWORD,
        "port": "5432",
    }

    KV_INFO = {
        "host": KV_HOST,
        "port": KV_PORT,
        "db": KV_DATABASE,
        "password": KV_PASSWORD,
    }

    # Creating db_handler (works with just one stock for now)
    ticker = "aapl"
    sma_db = DBRepository([ticker], DB_INFO, DB_HANDLER)
    kv_handler = KeyValueRepository(KV_INFO, KV_HANDLER)

    # Define a start pull date to run the simulation
    start_date = "2021-07-10"
    curr_date = str_to_dt(start_date)
    last_date_entry_str = sma_db.get_most_recent_date(ticker)
    last_date_entry = str_to_dt(last_date_entry_str)
    print(last_date_entry_str)

    # Running SMA for every day since the given start date up until the most
    # recent date in the DB
    while curr_date <= last_date_entry:
        print(curr_date)
        sma = SMACross(ticker, sma_db, kv_handler)
        result = sma.check_sma_cross()
        curr_date = curr_date + timedelta(days=1)
