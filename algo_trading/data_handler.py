import time
import sys
from typing import Union
from urllib.error import HTTPError

import pandas as pd
from dateutil.parser import parse


import ssl

ssl._create_default_https_context = ssl._create_unverified_context


class YahooFinanceDataHandler:
    def get_stock_data(
        self, symbol: str, start_date: str, end_date: str, interval: str
    ) -> pd.DataFrame:
        try:
            hist_data = pd.read_csv(
                self._create_query_string(symbol, start_date, end_date, interval)
            )
            return hist_data
        except HTTPError:
            sys.exit(
                f"\nCannot pull data for {symbol} for dates: {start_date} to {end_date}.\n"
            )

    def _create_query_string(
        self, symbol: str, start_date: str, end_date: str, interval: str
    ) -> str:
        if start_date == "max":
            start_period_in = 0000000000
        else:
            start_date_obj = parse(start_date)
            start_period_in = int(time.mktime(start_date_obj.timetuple()))

        end_date_obj = parse(end_date)
        end_period_in = int(time.mktime(end_date_obj.timetuple()))

        query_string = f"""https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={start_period_in}&period2={end_period_in}&interval={interval}&events=history&includeAdjustedClose=true"""
        return query_string


def get_data_handler(data_handler: str) -> Union[YahooFinanceDataHandler]:
    if data_handler == "yahoo_finance":
        return YahooFinanceDataHandler()
