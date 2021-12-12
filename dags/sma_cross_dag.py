import os
import json
from typing import Dict, List

import pandas as pd
import yaml as yl

from algo_trading.strategies.sma_cross_strat import SMACross
from algo_trading.repositories.db_repository import DBRepository
from algo_trading.repositories.key_val_repository import KeyValueRepository
from algo_trading.config.controllers import ColumnController, StockStatusController
from algo_trading.utils.utils import str_to_dt, dt_to_str
from algo_trading.config import DB_INFO, KV_INFO


"""
The purpose of this script is to run a flow of the DBRepository and DataRepository objects.
This is essentially a placeholder that will be replaced with a simple Airflow DAG,
in which each function will be a task.

Environment variables and config.yml will be depricated when moving to Airflow, since
we will slap them on the Airflow metastore.


TODO: Create a Logging mechanism
"""

# HOW TO GET THE NEW TICKERS AND OLD TICKERS PASSED FROM DATA_PULL_DAG HERE?


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
        cross_info = kv.get(ticker)
        if cross_info is not None:
            print(
                f"Redis data for {ticker} already exists bum! - resetting to empty..."
            )
        cross_info = dict()
        data = data.sort_values(
            [ColumnController.date.value], ascending=True
        ).reset_index(drop=True)

        # Dirty way of finding out last cross up and cross down - can def do better
        for i in range(1, len(data.index), -1):
            if SMACross.cross_up(data, i):
                cross_info[ColumnController.last_cross_up.value] = data[
                    ColumnController.date.value
                ].iloc[i]
                break

        for i in range(1, len(data.index), -1):
            if SMACross.cross_down(data, i):
                cross_info[ColumnController.last_cross_down.value] = data[
                    ColumnController.date.value
                ].iloc[i]
                break

        if str_to_dt(cross_info[ColumnController.last_cross_down.value]) < str_to_dt(
            cross_info[ColumnController.last_cross_up.value]
        ):
            cross_info[
                ColumnController.last_status.value
            ] = StockStatusController.buy.value
        else:
            cross_info[
                ColumnController.last_status.value
            ] = StockStatusController.sell.value

        kv.set(ticker, cross_info)
        print(f"Updated Redis for {ticker}: {json.dumps(cross_info, indent=2)}")


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
        cross_info = kv.get(ticker)

        if cross_info is None:
            raise ValueError(f"No Redis data for ticker {ticker}...")

        cross_info = json.loads(cross_info)
        print(f"Current Redis for {ticker}: {json.dumps(cross_info, indent=2)}")

        if SMACross.cross_up(data, 0):
            cross_info[ColumnController.last_cross_up.value] = dt_to_str(
                data[ColumnController.date.value].iloc[0]
            )

        elif SMACross.cross_down(data, 0):
            # Checks the case when we had a cross up in bear market
            if str_to_dt(
                cross_info[ColumnController.last_cross_down.value]
            ) < str_to_dt(cross_info[ColumnController.last_cross_up.value]):
                cross_info[ColumnController.last_cross_down.value] = dt_to_str(
                    data[ColumnController.date.value].iloc[0]
                )

        kv.set(ticker, cross_info)
        print(f"Updated Redis for {ticker}: {json.dumps(cross_info, indent=2)}")


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

    backfill_redis(KV_INFO, kv_handler, new_ticker_data)

    update_redis(DB_INFO, db_handler, KV_INFO, kv_handler, tickers, new_tickers)
