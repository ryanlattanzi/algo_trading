import os
from datetime import date, datetime, timedelta
from typing import Dict, List

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
DATE_FORMAT = "%Y-%m-%d"


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

        query_date = datetime.strptime(last_date_entry, DATE_FORMAT) + timedelta(days=1)
        query_date_str = query_date.strftime(DATE_FORMAT)

        end_date = date.today()
        end_date_str = end_date.strftime(DATE_FORMAT)

        print(f"last date entry for {ticker}: {last_date_entry}")
        print(f"pulling {ticker} from {query_date_str} to {end_date_str}")
        if not end_date.weekday() in [5, 6]:
            stock_df = data_handler.get_stock_data(
                ticker, query_date_str, end_date_str, "1d"
            )
            if stock_df is not None:
                stock_df = clean_df(stock_df)
                # One-off error found when the API returns data for a weekend
                if datetime.strptime(stock_df["date"][0], DATE_FORMAT) >= query_date:
                    print(
                        f"adding {ticker} data for dates "
                        + f"{stock_df['date'].iloc[0]} -> {stock_df['date'].iloc[-1]}"
                    )
                    updated_ticker_data[ticker] = stock_df
        else:
            print(f"{end_date_str} is a weekend! No run run today boo boo...")
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


if __name__ == "__main__":
    new_tickers = create_new_tables()
    new_ticker_data = get_new_ticker_data(new_tickers)
    add_new_ticker_data(new_ticker_data)

    existing_ticker_data = get_existing_ticker_data(new_tickers)
    add_existing_ticker_data(existing_ticker_data)
