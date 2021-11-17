from abc import ABC, abstractmethod
import time
import sys
from typing import Dict
from urllib.error import HTTPError
import ssl

import pandas as pd
from dateutil.parser import parse

from algo_trading.config.controllers import DataHandlerController


ssl._create_default_https_context = ssl._create_unverified_context


class AbstractDataRepository(ABC):
    @abstractmethod
    def get_stock_data(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
        interval: str,
    ) -> pd.DataFrame:
        pass


class YahooFinanceDataRepository(AbstractDataRepository):
    def __init__(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
        interval: str = "1d",
    ) -> None:
        """Data Repository that hits the Yahoo Finance API endpoint
        to get raw price data and return it as a pandas DF.

        Args:
            ticker (str): Ticker to fetch data.
            start_date (str): Get data starting here.
            end_date (str): Get data up until this point (not including).
            interval (str, optional): Interval of datapoints. Defaults to "1d".
        """
        self.ticker = ticker
        self.start_date = start_date
        self.end_date = end_date
        self.interval = interval

    @property
    def start_period_tuple(self) -> int:
        if self.start_date == "max":
            return 0000000000
        else:
            return int(time.mktime(parse(self.start_date).timetuple()))

    @property
    def end_period_tuple(self) -> int:
        return int(time.mktime(parse(self.end_date).timetuple()))

    @property
    def query_string(self) -> str:
        return (
            f"https://query1.finance.yahoo.com/v7/finance/download/{self.ticker}"
            + f"?period1={self.start_period_tuple}&period2={self.end_period_tuple}"
            + f"&interval={self.interval}&events=history&includeAdjustedClose=true"
        )

    def get_stock_data(self) -> pd.DataFrame:
        try:
            return pd.read_csv(self.query_string)
        except HTTPError:
            sys.exit(
                f"\nCannot pull data for {self.ticker} for dates: "
                + f"{self.start_date} to {self.end_date}.\n"
            )


class DataRepository:
    _data_handlers = {
        DataHandlerController.yahoo_finance: YahooFinanceDataRepository,
    }

    def __init__(self, data_info: Dict, data_handler: DataHandlerController) -> None:
        """A wrapper class to provide a consistent interface to the
        different DataRepository types found in the _data_handlers class
        attribute.

        Args:
            data_info (Dict): Info to splat into a DataRepository instance.
            data_handler (DataHandlerController): Type of handler to use.
        """
        self.data_info = data_info
        self.data_handler = data_handler

    @property
    def handler(self) -> AbstractDataRepository:
        return DataRepository._data_handlers[self.data_handler](**self.data_info)
