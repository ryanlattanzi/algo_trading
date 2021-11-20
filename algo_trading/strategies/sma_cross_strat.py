from typing import Tuple, Dict
import os
import yaml as yl
import json
import pandas as pd
import random

from datetime import datetime, timedelta

from algo_trading.strategies.abstract_strategy import AbstractStrategy
from algo_trading.strategies.events import TradeEvent
from algo_trading.config.controllers import ColumnController, StockStatusController
from algo_trading.repositories.db_repository import DBRepository
from algo_trading.repositories.key_val_repository import KeyValueRepository
from algo_trading.utils.utils import str_to_dt

"""Description of algorithm:


"""


class SMACross(AbstractStrategy):
    def __init__(
        self,
        ticker: str,
        sma_db: DBRepository,
        cross_db: KeyValueRepository,
    ) -> None:

        self.ticker = ticker
        self.sma_db = sma_db.handler
        self.cross_db = cross_db.handler

    @property
    def cross_info(self) -> Dict:
        return json.loads(self.cross_db.get(self.ticker))

    @property
    def sma_info(self) -> Dict:
        try:
            data = data.to_dict("records")[0]
        except IndexError:
            data = {ColumnController.date.value: None}
        return data

    def _update_last_status(self, signal: StockStatusController) -> None:
        current = self.cross_info
        current[ColumnController.last_status.value] = signal.value
        self.cross_db.set(self.ticker, current)
        print("Success")

    def run(self) -> Tuple[datetime.date, str, str]:
        last_cross_up_int = str_to_dt(
            self.cross_info[ColumnController.last_cross_up.value]
        )
        last_cross_down_int = str_to_dt(
            self.cross_info[ColumnController.last_cross_down.value]
        )
        last_status = self.cross_info[ColumnController.last_status.value]
        # print(last_cross_up_int, last_cross_down_int, last_status)

        date = self.sma_info[ColumnController.date.value]  # Current Date
        print(date)
        if date:
            if last_cross_up_int > last_cross_down_int:
                if last_status == StockStatusController.buy.value:
                    signal = StockStatusController.hold
                # elif last_status == StockStatusController.hold.value:
                #     signal = StockStatusController.hold
                # elif last_status == StockStatusController.wait.value:
                #     signal = StockStatusController.buy
                elif last_status == StockStatusController.sell.value:
                    signal = StockStatusController.buy
                    self._update_last_status(signal)
            else:
                if last_status == StockStatusController.buy.value:
                    signal = StockStatusController.sell
                    self._update_last_status(signal)
                # elif last_status == StockStatusController.hold.value:
                #     signal = StockStatusController.sell
                # elif last_status == StockStatusController.wait.value:
                #     signal = StockStatusController.wait
                elif last_status == StockStatusController.sell.value:
                    signal = StockStatusController.wait
                    # print(TradeEvent(date=date, ticker=self.ticker, signal=signal))
        else:
            signal = StockStatusController.wait

        # print(signal)

        return TradeEvent(date=date, ticker=self.ticker, signal=signal)


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
