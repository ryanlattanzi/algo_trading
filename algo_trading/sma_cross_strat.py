from typing import Tuple
import os
import yaml as yl
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta

from db_handler import get_db_handler
from controllers import ColumnController


load_dotenv("../local.env")


class SMACross:
    def __init__(self, ticker: str, slice: pd.DataFrame, days_back: int = 6) -> None:

        self.ticker = ticker
        self.slice = slice
        self.days_back = days_back

        self._validate_slice()

    def _validate_slice(self) -> None:
        if len(self.slice) != self.days_back:
            length = len(self.slice)
            raise IndexError(
                f"Given data slice has length {length}, but needs {self.days_back}"
            )

    def check_sma_cross(self) -> Tuple[datetime.date, str, str]:

        # buffer up should be greater than 1 and buffer down should be lower than 1 if you want to make breakout signals stronger
        buffer_up = 1
        buffer_down = 1
        date = self.slice["date"].iloc[0]

        # TODO: Need to figure out if the dataframe that I am analyzing is going to be in ascending or descending order. Currently it is in descending order when analyzing.
        if (
            self.slice["close"].iloc[0] <= self.slice["ma_50"].iloc[0]
            or self.slice["ma_50"].iloc[0] <= self.slice["ma_200"].iloc[0]
        ):
            return (date, self.ticker, "BEAR MARKET")
        else:
            if (
                buffer_up * (self.slice["ma_7"].iloc[0]) >= self.slice["ma_21"].iloc[0]
            ) and (self.slice["ma_7"].iloc[1] < self.slice["ma_21"].iloc[1]):
                return (date, self.ticker, "BUY")

            elif (
                buffer_down * (self.slice["ma_7"].iloc[0]) < self.slice["ma_21"].iloc[0]
            ) and (self.slice["ma_7"].iloc[1] >= self.slice["ma_21"].iloc[1]):
                return (date, self.ticker, "SELL")
            else:
                return (date, self.ticker, "HOLD")


if __name__ == "__main__":
    # Loading in DB info
    HOST = os.getenv("POSTGRES_HOST")
    DATABASE = os.getenv("POSTGRES_DB")
    USER = os.getenv("POSTGRES_USER")
    PASSWORD = os.getenv("POSTGRES_PASSWORD")

    # Loading in and parsing CONFIG
    CONFIG = yl.safe_load(open("config.yml", "r"))
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

    # Creating db_handler (works with just one stock for now)
    ticker = "aapl"
    db_handler = get_db_handler(DB_HANDLER, [ticker], DB_INFO)

    # Define a start pull date to run the simulation
    start_date = "2021-08-01"
    curr_date = datetime.strptime(start_date, DATE_FORMAT)

    last_date_entry = db_handler.get_most_recent_date(ticker)

    # Running SMA for every day since the given start date up until the most
    # recent date in the DB
    while curr_date <= datetime(
        last_date_entry.year, last_date_entry.month, last_date_entry.day
    ):
        curr_date_str = curr_date.strftime(DATE_FORMAT)
        days_back = 6
        condition = (
            f"WHERE date <= '{curr_date_str}' ORDER BY date DESC LIMIT {days_back}"
        )
        slice = db_handler.get_data(ticker, condition)
        sma = SMACross(ticker, slice, days_back)
        result = sma.check_sma_cross()
        print(result)
        curr_date = curr_date + timedelta(days=1)
