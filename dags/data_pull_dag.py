import os
from io import StringIO
from datetime import datetime, timedelta
from typing import Dict, List
import pandas as pd

from algo_trading.logger.default_logger import get_main_logger
from algo_trading.logger.controllers import LogLevelController
from algo_trading.utils.calculations import Calculator
from algo_trading.repositories.data_repository import DataRepository
from algo_trading.repositories.db_repository import DBRepository
from algo_trading.repositories.obj_store_repository import ObjStoreRepository
from algo_trading.config.controllers import (
    ColumnController,
    DBHandlerController,
    ObjStoreController,
)
from algo_trading.utils.utils import clean_df, str_to_dt, dt_to_str
from algo_trading.config import (
    DB_INFO,
    DATE_FORMAT,
    OBJ_STORE_INFO,
    CONFIG,
    DATA_BUCKET,
    DATA_KEY,
    LOG_BUCKET,
    LOG_KEY,
)


LOG, LOG_INFO = get_main_logger(
    log_name="data_pull_dag",
    file_name=os.path.join("logs", f"data_pull_dag_{dt_to_str(datetime.today())}.log"),
    log_level=LogLevelController.info,
)

DB_HANDLER = DBRepository(
    DB_INFO,
    DBHandlerController[CONFIG.db_repo],
    LOG_INFO,
).handler

OBJ_STORE_HANDLER = ObjStoreRepository(
    OBJ_STORE_INFO,
    ObjStoreController[CONFIG.obj_store_repo],
    LOG_INFO,
).handler


def create_bucket(bucket_name: str) -> None:
    """Creates bucket if not exists.

    Args:
        bucket_name (str): Name of bucket to create.
    """
    buckets = OBJ_STORE_HANDLER.list_buckets()
    if bucket_name not in [bucket["Name"] for bucket in buckets["Buckets"]]:
        LOG.info(f"Creating bucket {bucket_name}")
        OBJ_STORE_HANDLER.create_bucket(bucket_name)


def create_new_tables(tickers: List) -> List:
    """
    Initializes the config DBInterface instance and creates new tables
    based off of new tickers that popped up in the configuration.

    Returns:
        List: List of new tickers
    """
    new_tickers = DB_HANDLER.create_new_ticker_tables(tickers)
    return new_tickers


def get_new_ticker_data(
    data_handler: str,
    new_tickers: List,
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
        "log_info": LOG_INFO,
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


def persist_ticker_data(
    ticker_data: Dict[str, pd.DataFrame]
) -> Dict[str, Dict[str, str]]:
    """
    Slaps the historical data from pd.DataFrame into the DB and Object
    Store.

    Args:
        ticker_data (Dict): Data to be loaded to the DB

    Returns:
        Dict: Mapping tickers to bucket and key.
    """

    paths = dict()
    for ticker, df in ticker_data.items():
        # Persisting data to object storage.
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        bucket = DATA_BUCKET
        key = DATA_KEY.format(ticker=ticker, run_date=dt_to_str(datetime.today()))
        OBJ_STORE_HANDLER.put_object(
            file_body=csv_buffer.getvalue(),
            bucket=bucket,
            key=key,
        )
        paths[ticker] = {
            "bucket": bucket,
            "key": key,
        }

        # Saving data to the database.
        DB_HANDLER.append_df_to_sql(ticker, df)

    return paths


def get_existing_ticker_data(
    data_handler: str,
    tickers: List,
    new_tickers: List,
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

    for ticker in [t for t in tickers if t not in new_tickers]:
        last_date_entry_str = DB_HANDLER.get_most_recent_date(ticker)
        last_date_entry = str_to_dt(last_date_entry_str)

        query_date = last_date_entry + timedelta(days=1)
        query_date_str = dt_to_str(query_date)

        end_date = datetime.today()
        end_date_str = end_date.strftime(DATE_FORMAT)

        # if end_date.weekday() in [5, 6]:
        #     # make a custom exception here
        #     sys.exit(f"{end_date_str} is a weekend! No run run today boo boo...")

        LOG.info(f"Last date entry for {ticker}: {last_date_entry_str}")

        if query_date_str == end_date_str:
            LOG.info(f"Data for {ticker} already up to date on {end_date_str}.")
            continue

        LOG.info(f"Pulling {ticker} from {query_date_str} to {end_date_str}")

        data_pull_params = {
            "ticker": ticker,
            "start_date": query_date_str,
            "end_date": end_date_str,
            "log_info": LOG_INFO,
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
            LOG.error(
                f"First date of stock_df {stock_df_first_date} is "
                + f"less than query date: {query_date_str} for {ticker}"
            )
            continue

        LOG.info(
            f"Adding {ticker} data for dates "
            + f"{stock_df[ColumnController.date.value].iloc[0]}"
            + f" -> {stock_df[ColumnController.date.value].iloc[-1]}"
        )

        # Getting rows 199 days back from the earliest row of the DF
        hist_df = DB_HANDLER.get_days_back(ticker, 199)
        full_df = pd.concat([hist_df, stock_df], axis=0).reset_index(drop=True)
        full_df[ColumnController.date.value] = pd.to_datetime(
            full_df[ColumnController.date.value]
        )

        full_df = Calculator.calculate_sma(full_df, ColumnController.close.value)

        # Only getting the new rows to upload to the DB
        mask = full_df[ColumnController.date.value] >= stock_df_first_date
        updated_ticker_data[ticker] = full_df.loc[mask]
    return updated_ticker_data


def persist_log() -> None:
    OBJ_STORE_HANDLER.upload_file(
        LOG_INFO.file_name,
        LOG_BUCKET,
        LOG_KEY.format(
            log_name=LOG_INFO.log_name, run_date=dt_to_str(datetime.today())
        ),
    )


def finish_log() -> None:
    LOG.info(f"END OF DATA PULL DAG ON {dt_to_str(datetime.today())}.\n")
