import os
from datetime import date
from typing import Dict, List, Tuple, Union

import yaml as yl
from dotenv import load_dotenv

from data_handler import get_data_handler
from db_handler import get_db_handler
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
TODO: FOR CHRIS - Finish function in utils.py
"""

# Loading in DB info
HOST = os.getenv("POSTGRES_HOST")
DATABASE = os.getenv("POSTGRES_DB")
USER = os.getenv("POSTGRES_USER")
PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Loading in Tickers config
CONFIG = yl.safe_load(open("config.yml", "r"))

# Parsing configs/building variables for processing
TICKERS = CONFIG["ticker_list"]
DB_HANDLER = CONFIG["db_handler"]
DATA_HANDLER = CONFIG["data_handler"]
DB_INFO = {
    "host": HOST,
    "database": DATABASE,
    "user": USER,
    "password": PASSWORD,
    "port": "5432",
}


def create_new_tables():
    """
    Initializes the config DBInterface instance and creates new tables
    based off of new tickers that popped up in the configuration.

    Returns:
        Tuple[DBInterface, List]: DBInterface object and list of new tickers
    """
    handler = get_db_handler(DB_HANDLER)
    db = handler(TICKERS, DB_INFO)
    new_tickers = db.create_new_ticker_tables()
    return db, new_tickers


def get_new_ticker_data(new_tickers):
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
            ticker, "max", date.today().strftime("%m-%d-%Y"), "1d"
        )
        stock_df = clean_df(stock_df)
        # print(stock_df.columns)
        # print(clean_df)
        new_ticker_data[ticker] = stock_df
    return new_ticker_data


def backfill_new_tickers(db, new_ticker_data):
    """
    Slaps the historical data from pd.DataFrame into the DB.
    For now, this is meant to run for new tickers, since it will
    load the entire DF into the table.

    Args:
        db (DBInterface): Connector to the DB
        new_ticker_data (Dict): New data to be loaded to the DB
    """
    for ticker, df in new_ticker_data.items():
        db.add_hist_data(ticker, df)


# TODO: Function will create a dictionary with ticker as the key and pd.dataframe as the keys.
# The dataframe date will start with the last date entry and end on the current day
def get_updated_ticker_data():
    updated_ticker_data = dict()
    data_handler = get_data_handler(DATA_HANDLER)
    db_handler = get_db_handler(DB_HANDLER)

    for ticker in TICKERS:
        last_date_entry = db_handler.get_most_recent_date(ticker)
        current_date = date.today().strftime("%m-%d-%Y")
        interval = "1d"
        stock_df = data_handler.get_stock_data(
            ticker, last_date_entry, current_date, interval
        )
        stock_df = clean_df(stock_df)
        updated_ticker_data[ticker] = stock_df

    return updated_ticker_data


# TODO: Function will append updated ticker data to existing tables in database
def append_updated_ticker_data(updated_ticker_data):
    db_handler = get_db_handler(DB_HANDLER)
    for ticker, df in updated_ticker_data.items():
        db_handler.add_new_data(ticker, df)


if __name__ == "__main__":
    db, new_tickers = create_new_tables()
    new_ticker_data = get_new_ticker_data(new_tickers)
    updated_ticker_data = get_updated_ticker_data()
    backfill_new_tickers(db, new_ticker_data)
    append_updated_ticker_data(updated_ticker_data)
