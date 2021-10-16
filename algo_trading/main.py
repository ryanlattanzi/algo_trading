import os
import sys
import json
from datetime import date, datetime, timedelta
from typing import Dict, List

import pandas as pd
import yaml as yl
from dotenv import load_dotenv

from utils.calculations import Calculator
from handlers.data_handler import DataRepository
from handlers.db_handler import DBRepository
from handlers.in_memory_handler import get_in_memory_handler
from config.controllers import ColumnController
from utils.utils import clean_df, str_to_dt, dt_to_str
from constants import DATE_FORMAT

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
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Loading in IN MEMORY info
IN_MEM_HOST = os.getenv("REDIS_HOST")
IN_MEM_PORT = os.getenv("REDIS_PORT")
IN_MEM_DATABASE = os.getenv("REDIS_DB")
IN_MEM_PASSWORD = os.getenv("REDIS_PASSWORD")

# Loading in and parsing CONFIG
CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "config/config.yml"
)
CONFIG = yl.safe_load(open(CONFIG_PATH, "r"))
TICKERS = CONFIG["ticker_list"]
DB_HANDLER = CONFIG["db_handler"]
DATA_HANDLER = CONFIG["data_handler"]
IN_MEM_HANDLER = CONFIG["in_memory_handler"]

# Building global vars for processing

DB_INFO = {
    "host": DB_HOST,
    "db_name": DB_NAME,
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
    db_handler = DBRepository(TICKERS, DB_INFO, DB_HANDLER)
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
    data_pull_params = {
        "start_date": "max",
        "end_date": dt_to_str(datetime.today()),
    }

    for ticker in new_tickers:
        data_pull_params["ticker"] = ticker
        stock_df = DataRepository(
            data_pull_params, DATA_HANDLER
        ).handler.get_stock_data()
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
    db_handler = DBRepository(TICKERS, DB_INFO, DB_HANDLER)

    for ticker, df in new_ticker_data.items():
        db_handler.append_df_to_sql(ticker, df)


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
    db_handler = DBRepository(TICKERS, DB_INFO, DB_HANDLER)

    for ticker in [t for t in TICKERS if t not in new_tickers]:
        last_date_entry_str = db_handler.get_most_recent_date(ticker)
        last_date_entry = str_to_dt(last_date_entry_str)

        query_date = last_date_entry + timedelta(days=1)
        query_date_str = dt_to_str(query_date)

        end_date = datetime.today()
        end_date_str = end_date.strftime(DATE_FORMAT)

        if end_date.weekday() in [5, 6]:
            # make a custom exception here
            sys.exit(f"{end_date_str} is a weekend! No run run today boo boo...")

        print(f"Last date entry for {ticker}: {last_date_entry_str}")
        print(f"Pulling {ticker} from {query_date_str} to {end_date_str}")

        data_pull_params = {
            "ticker": ticker,
            "start_date": query_date_str,
            "end_date": end_date_str,
        }

        stock_df = DataRepository(
            data_pull_params, DATA_HANDLER
        ).handler.get_stock_data()
        stock_df = clean_df(stock_df)
        stock_df = stock_df.sort_values([ColumnController.date.value], ascending=True)
        stock_df_first_date = stock_df[ColumnController.date.value][0]

        # One-off error found when the API returns data for a weekend
        # Only process the data if the first date of the returned DF is
        # greater than or equal to the query_date.
        if str_to_dt(stock_df_first_date) < query_date:
            raise ValueError(
                f"First date of stock_df is less than query date: {query_date_str}"
            )

        print(
            f"Adding {ticker} data for dates "
            + f"{stock_df[ColumnController.date.value].iloc[0]}"
            + f"-> {stock_df[ColumnController.date.value].iloc[-1]}"
        )

        # Getting rows 199 days back from the earliest row of the DF
        hist_df = db_handler.get_days_back(ticker, 199)
        hist_df = hist_df.sort_values([ColumnController.date.value], ascending=True)
        full_df = pd.concat([hist_df, stock_df], axis=0).reset_index(drop=True)
        full_df[ColumnController.date.value] = pd.to_datetime(
            full_df[ColumnController.date.value]
        )

        full_df = Calculator.calculate_sma(full_df, ColumnController.close.value)

        # Only getting the new rows to upload to the DB
        mask = full_df[ColumnController.date.value] >= stock_df_first_date
        updated_ticker_data[ticker] = full_df.loc[mask]
    return updated_ticker_data


def add_existing_ticker_data(existing_ticker_data: Dict) -> None:
    """
    Append updated ticker data to existing tables in the DB.

    Args:
        existing_ticker_data (Dict): Data to be loaded to the DB
    """
    db_handler = DBRepository(TICKERS, DB_INFO, DB_HANDLER)

    for ticker, df in existing_ticker_data.items():
        db_handler.append_df_to_sql(ticker, df)


def update_redis() -> None:
    db_handler = DBRepository(TICKERS, DB_INFO, DB_HANDLER)
    in_mem_handler = get_in_memory_handler(IN_MEM_HANDLER, IN_MEMORY_INFO)

    for ticker in TICKERS:
        data = db_handler.get_days_back(ticker, 2)
        current_data = in_mem_handler.get(ticker)

        if current_data is None:
            print(f"No Redis data for {ticker}. Creating now.")
            current_data = {
                "last_cross_up": None,
                "last_cross_down": dt_to_str(data["date"].iloc[0]),
                "last_status": "SELL",
            }
        else:
            current_data = json.loads(current_data)
            print(f"Current Redis for {ticker}: {json.dumps(current_data, indent=2)}")

        if (data["ma_7"].iloc[0] >= data["ma_21"].iloc[0]) and (
            data["ma_7"].iloc[1] < data["ma_21"].iloc[1]
        ):
            if data["close"].iloc[0] > data["ma_50"].iloc[0]:
                current_data["last_cross_up"] = dt_to_str(data["date"].iloc[0])

        elif (data["ma_7"].iloc[0] < data["ma_21"].iloc[0]) and (
            data["ma_7"].iloc[1] >= data["ma_21"].iloc[1]
        ):
            if str_to_dt(current_data["last_cross_down"]) < str_to_dt(
                current_data["last_cross_up"]
            ):
                current_data["last_cross_down"] = dt_to_str(data["date"].iloc[0])

        in_mem_handler.set(ticker, current_data)
        print(f"Updated Redis for {ticker}: {json.dumps(current_data, indent=2)}")


if __name__ == "__main__":
    new_tickers = create_new_tables()
    new_ticker_data = get_new_ticker_data(new_tickers)
    add_new_ticker_data(new_ticker_data)

    existing_ticker_data = get_existing_ticker_data(new_tickers)
    add_existing_ticker_data(existing_ticker_data)

    update_redis()
