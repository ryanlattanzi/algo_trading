import os
import sys
import json
from datetime import date, datetime, timedelta
from typing import Dict, List

import pandas as pd
import yaml as yl
from dotenv import load_dotenv

from calculations import Calculator
from data_handler import get_data_handler
from db_handler import get_db_handler
from in_memory_handler import get_in_memory_handler
from controllers import ColumnController
from utils import clean_df

load_dotenv("../local.env")

"""
The purpose of this script is to run a flow of the DbHandler and StockData objects.
This is essentially a placeholder that will be replaced with a simple Airflow DAG,
in which each function will be a task. We could really get into it and eventually
Dockerize each task (I think this will be a good idea even though it's overkill)

Environment variables and config.yml will be depricated when moving to Airflow, since
we will slap those bitches on the Airflow metastore.

Also, for both db_handler and stock_data, we will create what is called an interface
using 'Abstract Base Class' (ABC), so that no matter which DB we connect to (MySQL, Postgres, etc),
there will always be the same function names, but different function implementations.
Similarly, we can do the same for stock_data for wherever we pull data (Yahoo, Binance, etc).

TODO: Write a testing framework that interacts with the dev db
TODO: Create a Logging mechanism
"""

# Loading in DB info
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_HOST")
DB_DATABASE = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Loading in IN MEMORY info
IN_MEM_HOST = os.getenv("REDIS_HOST")
IN_MEM_PORT = os.getenv("REDIS_PORT")
IN_MEM_DATABASE = os.getenv("REDIS_DB")
IN_MEM_PASSWORD = os.getenv("REDIS_PASSWORD")

# Loading in and parsing CONFIG
CONFIG = yl.safe_load(open("config.yml", "r"))
TICKERS = CONFIG["ticker_list"]
DB_HANDLER = CONFIG["db_handler"]
DATA_HANDLER = CONFIG["data_handler"]
IN_MEM_HANDLER = CONFIG["in_memory_handler"]

# Building global vars for processing
DATE_FORMAT = "%Y-%m-%d"
DB_INFO = {
    "host": DB_HOST,
    "database": DB_DATABASE,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "port": "5432",
}
IN_MEMORY_INFO = {
    "host": IN_MEM_HOST,
    "port": IN_MEM_PORT,
    "db": IN_MEM_DATABASE,
    "password": IN_MEM_PASSWORD,
}


def create_new_tables() -> List:
    """
    Initializes the config DBInterface instance and creates new tables
    based off of new tickers that popped up in the configuration.

    Returns:
        List: List of new tickers
    """
    db_handler = get_db_handler(DB_HANDLER, TICKERS, DB_INFO)

    new_tickers = db_handler.create_new_ticker_tables()
    return new_tickers


def get_new_ticker_data(new_tickers: List) -> Dict:
    """
    Gets historical data for the list of new tickers found.
    Does not put into the DB, but keeps it in memory for the
    next task. We could think about persisting this data to
    disk or to AWS or something.

    Args:
        new_tickers (List): New tickers

    Returns:
        Dict: New data with key: ticker, val: pd.DataFrame
    """
    new_ticker_data = dict()
    data_handler = get_data_handler(DATA_HANDLER)

    for ticker in new_tickers:
        stock_df = data_handler.get_stock_data(
            ticker, "max", date.today().strftime(DATE_FORMAT), "1d"
        )
        stock_df = clean_df(stock_df)
        stock_df = stock_df.sort_values([ColumnController.date.value], ascending=True)
        stock_df = Calculator.calculate_sma(stock_df, ColumnController.close.value)

        new_ticker_data[ticker] = stock_df
    return new_ticker_data


def add_new_ticker_data(new_ticker_data: Dict) -> None:
    """
    Slaps the historical data from pd.DataFrame into the DB.
    For now, this is meant to run for new tickers, since it will
    load the entire DF into the table.

    Args:
        new_ticker_data (Dict): New data to be loaded to the DB
    """
    db_handler = get_db_handler(DB_HANDLER, TICKERS, DB_INFO)

    for ticker, df in new_ticker_data.items():
        db_handler.df_to_sql(ticker, df)


def get_existing_ticker_data(new_tickers: List) -> Dict:
    """
    Gets data for the list of existing tickers. The DF date will
    start with the day after the last date entry (inclusive) and end
    on the current day (exclusive).

    Does not put into the DB, but keeps it in memory for the
    next task. We could think about persisting this data to
    disk or to AWS or something.

    Args:
        new_tickers (List): New tickers to exclude from pulling data

    Returns:
        Dict: Existing data with key: ticker, val: pd.DataFrame
    """
    updated_ticker_data = dict()
    db_handler = get_db_handler(DB_HANDLER, TICKERS, DB_INFO)
    data_handler = get_data_handler(DATA_HANDLER)

    for ticker in [t for t in TICKERS if t not in new_tickers]:
        last_date_entry = db_handler.get_most_recent_date(ticker)

        query_date = last_date_entry + timedelta(days=1)
        query_date_str = query_date.strftime(DATE_FORMAT)

        end_date = date.today()
        end_date_str = end_date.strftime(DATE_FORMAT)

        if end_date.weekday() in [5, 6]:
            sys.exit(f"{end_date_str} is a weekend! No run run today boo boo...")

        print(f"Last date entry for {ticker}: {last_date_entry}")
        print(f"Pulling {ticker} from {query_date_str} to {end_date_str}")

        stock_df = data_handler.get_stock_data(
            ticker, query_date_str, end_date_str, "1d"
        )
        stock_df = clean_df(stock_df)
        stock_df = stock_df.sort_values([ColumnController.date.value], ascending=True)

        # One-off error found when the API returns data for a weekend
        # Only process the data if the first date of the returned DF is
        # greater than or equal to the query_date.
        if datetime.strptime(
            stock_df[ColumnController.date.value][0], DATE_FORMAT
        ) < datetime(query_date.year, query_date.month, query_date.day):
            raise ValueError(
                f"First date of stock_df is greater than query date: {query_date_str}"
            )

        print(
            f"adding {ticker} data for dates "
            + f"{stock_df[ColumnController.date.value].iloc[0]}"
            + f"-> {stock_df[ColumnController.date.value].iloc[-1]}"
        )

        # Getting rows 199 days back from the earliest row of the DF
        hist_df = db_handler.get_data(ticker, condition="ORDER BY date DESC LIMIT 199")
        hist_df = hist_df.sort_values([ColumnController.date.value], ascending=True)
        full_df = pd.concat([hist_df, stock_df], axis=0).reset_index(drop=True)
        full_df[ColumnController.date.value] = pd.to_datetime(
            full_df[ColumnController.date.value]
        )

        full_df = Calculator.calculate_sma(full_df, ColumnController.close.value)

        # Only getting the new rows to upload to the DB
        mask = (
            full_df[ColumnController.date.value]
            >= stock_df[ColumnController.date.value][0]
        )
        updated_ticker_data[ticker] = full_df.loc[mask]
    return updated_ticker_data


def add_existing_ticker_data(existing_ticker_data: Dict) -> None:
    """
    Append updated ticker data to existing tables in the DB.

    Args:
        existing_ticker_data (Dict): Data to be loaded to the DB
    """
    db_handler = get_db_handler(DB_HANDLER, TICKERS, DB_INFO)

    for ticker, df in existing_ticker_data.items():
        db_handler.df_to_sql(ticker, df)

def update_redis() -> None:
    db_handler = get_db_handler(DB_HANDLER, TICKERS, DB_INFO)
    in_mem_handler = get_in_memory_handler(IN_MEM_HANDLER, IN_MEMORY_INFO)

    for ticker in TICKERS:
        data = db_handler.get_data(ticker, condition='ORDER BY DATE DESC LIMIT 2')
        current_data = json.loads(in_mem_handler.get(ticker))

        if current_data is None:
            current_data = {'last_cross_up': None,
                            'cross_up_market_cond': None,
                            'last_cross_down': None}

        if ((data["ma_7"].iloc[0] >= data["ma_21"].iloc[0])
            and (data["ma_7"].iloc[1] < data["ma_21"].iloc[1])):
            current_data['last_cross_up'] = data['date'].iloc[0].strftime(DATE_FORMAT)
            if (
                    data["ma_21"].iloc[0] <= data["ma_50"].iloc[0]
                    or data["ma_50"].iloc[0] <= data["ma_200"].iloc[0]
            ):
                current_data['cross_up_market_cond'] = 'bear'
            else:
                current_data['cross_up_market_cond'] = 'bull'
        elif ((data["ma_7"].iloc[0] < data["ma_21"].iloc[0])
            and (data["ma_7"].iloc[1] >= data["ma_21"].iloc[1])):
            current_data['last_cross_down'] = data['date'].iloc[0].strftime(DATE_FORMAT)

        in_mem_handler.set(ticker, current_data)


if __name__ == "__main__":
    new_tickers = create_new_tables()
    new_ticker_data = get_new_ticker_data(new_tickers)
    add_new_ticker_data(new_ticker_data)

    existing_ticker_data = get_existing_ticker_data(new_tickers)
    add_existing_ticker_data(existing_ticker_data)
