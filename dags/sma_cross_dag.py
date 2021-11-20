import os
import sys
import json
from datetime import date, datetime, timedelta
from typing import Dict, List

import pandas as pd
import yaml as yl

from algo_trading.utils.calculations import Calculator
from algo_trading.repositories.data_repository import DataRepository
from algo_trading.repositories.db_repository import DBRepository
from algo_trading.repositories.key_val_repository import KeyValueRepository
from algo_trading.config.controllers import ColumnController, StockStatusController
from algo_trading.utils.utils import clean_df, str_to_dt, dt_to_str
from algo_trading.constants import DB_INFO, KV_INFO, DATE_FORMAT


"""
The purpose of this script is to run a flow of the DBRepository and DataRepository objects.
This is essentially a placeholder that will be replaced with a simple Airflow DAG,
in which each function will be a task.

Environment variables and config.yml will be depricated when moving to Airflow, since
we will slap them on the Airflow metastore.


TODO: Create a Logging mechanism
"""


def create_new_tables(db_info: Dict, db_handler: str, tickers: List) -> List:
    """
    Initializes the config DBInterface instance and creates new tables
    based off of new tickers that popped up in the configuration.

    Returns:
        List: List of new tickers
    """
    db = DBRepository(db_info, db_handler).handler
    new_tickers = db.create_new_ticker_tables(tickers)
    return new_tickers


def get_new_ticker_data(
    data_handler: str, new_tickers: List
) -> Dict[str, pd.DataFrame]:
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
            data_pull_params, data_handler
        ).handler.get_stock_data()
        stock_df = clean_df(stock_df)
        stock_df = stock_df.sort_values([ColumnController.date.value], ascending=True)
        stock_df = Calculator.calculate_sma(stock_df, ColumnController.close.value)

        new_ticker_data[ticker] = stock_df
    return new_ticker_data


def cross_up(data: pd.DataFrame, index: int) -> bool:
    """Checks to see if a cross up occured by looking at
    the current date 7 and 21 day SMA and the previous date
    7 and 21 day SMA. Finally, we only consider cross up
    when the close price > 50 ay SMA otherwise the market
    is considered bearish.

    Args:
        data (pd.DataFrame): Data to parse SMA info.
        index (int): Indicates current day. index + 1 = prev day.

    Returns:
        bool: True if all conditions are met.
    """
    return (
        (
            data[ColumnController.ma_7.value].iloc[index]
            >= data[ColumnController.ma_21.value].iloc[index]
        )
        and (
            data[ColumnController.ma_7.value].iloc[index + 1]
            < data[ColumnController.ma_21.value].iloc[index + 1]
        )
        and (
            data[ColumnController.close.value].iloc[index]
            > data[ColumnController.ma_50.value].iloc[index]
        )
    )


def cross_down(data: pd.DataFrame, index: int) -> bool:
    """Checks to see if a cross down occured by looking at
    the current date 7 and 21 day SMA and the previous date
    7 and 21 day SMA.

    Args:
        data (pd.DataFrame): Data to parse SMA info.
        index (int): Indicates current day. index + 1 = prev day.

    Returns:
        bool: True if all conditions are met.
    """
    return (
        data[ColumnController.ma_7.value].iloc[index]
        < data[ColumnController.ma_21.value].iloc[index]
    ) and (
        data[ColumnController.ma_7.value].iloc[index + 1]
        >= data[ColumnController.ma_21.value].iloc[index + 1]
    )


def add_new_ticker_data(
    db_info: Dict, db_handler: Dict, new_ticker_data: Dict[str, pd.DataFrame]
) -> None:
    """
    Slaps the historical data from pd.DataFrame into the DB.
    For now, this is meant to run for new tickers, since it will
    load the entire DF into the table.

    Args:
        new_ticker_data (Dict): New data to be loaded to the DB
    """
    db = DBRepository(db_info, db_handler).handler

    for ticker, df in new_ticker_data.items():
        db.append_df_to_sql(ticker, df)


def backfill_redis(
    kv_info: Dict, kv_handler: str, new_ticker_data: Dict[str, pd.DataFrame]
) -> None:
    """Gets up to date redis data for new tickers to indicate last
    cross dates and status.

    Args:
        new_ticker_data (Dict): New tickers to update redis with.
    """
    kv = KeyValueRepository(kv_info, kv_handler).handler
    for ticker, data in new_ticker_data.items():
        current_data = kv.get(ticker)
        if current_data is not None:
            # raise ValueError(f"Redis data for {ticker} already exists bum!")
            print(
                f"Redis data for {ticker} already exists bum! - resetting to empty..."
            )
        current_data = dict()
        data = data.sort_values([ColumnController.date.value], ascending=False)

        # Dirty way of finding out last cross up and cross down - can def do better
        for i in range(len(data.index) - 1):
            if cross_up(data, i):
                current_data[ColumnController.last_cross_up.value] = data[
                    ColumnController.date.value
                ].iloc[i]
                break

        for i in range(len(data.index) - 1):
            if cross_down(data, i):
                current_data[ColumnController.last_cross_down.value] = data[
                    ColumnController.date.value
                ].iloc[i]
                break

        if str_to_dt(current_data[ColumnController.last_cross_down.value]) < str_to_dt(
            current_data[ColumnController.last_cross_up.value]
        ):
            current_data[
                ColumnController.last_status.value
            ] = StockStatusController.buy.value
        else:
            current_data[
                ColumnController.last_status.value
            ] = StockStatusController.sell.value

        kv.set(ticker, current_data)
        print(f"Updated Redis for {ticker}: {json.dumps(current_data, indent=2)}")


def get_existing_ticker_data(
    db_info: Dict, db_handler: str, data_handler: str, tickers: List, new_tickers: List
) -> Dict:
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
    db = DBRepository(db_info, db_handler).handler

    for ticker in [t for t in tickers if t not in new_tickers]:
        last_date_entry_str = db.get_most_recent_date(ticker)
        last_date_entry = str_to_dt(last_date_entry_str)

        query_date = last_date_entry + timedelta(days=1)
        query_date_str = dt_to_str(query_date)

        end_date = datetime.today()
        end_date_str = end_date.strftime(DATE_FORMAT)

        # if end_date.weekday() in [5, 6]:
        #     # make a custom exception here
        #     sys.exit(f"{end_date_str} is a weekend! No run run today boo boo...")

        print(f"Last date entry for {ticker}: {last_date_entry_str}")
        print(f"Pulling {ticker} from {query_date_str} to {end_date_str}")

        data_pull_params = {
            "ticker": ticker,
            "start_date": query_date_str,
            "end_date": end_date_str,
        }

        stock_df = DataRepository(
            data_pull_params, data_handler
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
            + f" -> {stock_df[ColumnController.date.value].iloc[-1]}"
        )

        # Getting rows 199 days back from the earliest row of the DF
        hist_df = db.get_days_back(ticker, 199)
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


def add_existing_ticker_data(
    db_info: Dict, db_handler: Dict, existing_ticker_data: Dict
) -> None:
    """
    Append updated ticker data to existing tables in the DB.

    Args:
        existing_ticker_data (Dict): Data to be loaded to the DB
    """
    db = DBRepository(db_info, db_handler).handler

    for ticker, df in existing_ticker_data.items():
        db.append_df_to_sql(ticker, df)


def update_redis(
    db_info: Dict,
    db_handler: str,
    kv_info: Dict,
    kv_handler: str,
    tickers: List,
    new_tickers: List,
) -> None:
    """
    Updates redis information for existing tickers. Includes
    last cross up date and last cross down date.

    Args:
        new_tickers (List): New tickers. Used for exclusion.

    Raises:
        ValueError: Error raised if no redis data for the ticker.
    """
    db = DBRepository(db_info, db_handler).handler
    kv = KeyValueRepository(kv_info, kv_handler).handler

    for ticker in [t for t in tickers if t not in new_tickers]:
        data = db.get_days_back(ticker, 2)
        current_data = kv.get(ticker)

        if current_data is None:
            raise ValueError(f"No Redis data for ticker {ticker}...")

        current_data = json.loads(current_data)
        print(f"Current Redis for {ticker}: {json.dumps(current_data, indent=2)}")

        if cross_up(data, 0):
            current_data[ColumnController.last_cross_up.value] = dt_to_str(
                data[ColumnController.date.value].iloc[0]
            )

        elif cross_down(data, 0):
            if str_to_dt(
                current_data[ColumnController.last_cross_down.value]
            ) < str_to_dt(current_data[ColumnController.last_cross_up.value]):
                current_data[ColumnController.last_cross_down.value] = dt_to_str(
                    data[ColumnController.date.value].iloc[0]
                )

        kv.set(ticker, current_data)
        print(f"Updated Redis for {ticker}: {json.dumps(current_data, indent=2)}")


if __name__ == "__main__":

    # Loading in and parsing config
    config_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)).replace("dags", "algo_trading"),
        "config/config.yml",
    )
    config = yl.safe_load(open(config_path, "r"))
    tickers = config["ticker_list"]
    db_handler = config["db_repo"]
    data_handler = config["data_repo"]
    kv_handler = config["kv_repo"]

    new_tickers = create_new_tables(DB_INFO, db_handler, tickers)

    new_ticker_data = get_new_ticker_data(data_handler, new_tickers)
    add_new_ticker_data(DB_INFO, db_handler, new_ticker_data)
    backfill_redis(KV_INFO, kv_handler, new_ticker_data)

    existing_ticker_data = get_existing_ticker_data(
        DB_INFO, db_handler, data_handler, tickers, new_tickers
    )
    add_existing_ticker_data(DB_INFO, db_handler, existing_ticker_data)
    update_redis(DB_INFO, db_handler, KV_INFO, kv_handler, tickers, new_tickers)
