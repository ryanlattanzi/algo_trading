import os
import json
from typing import Dict, List
from datetime import datetime

from algo_trading.logger.default_logger import get_main_logger
from algo_trading.logger.controllers import LogLevelController
from algo_trading.strategies.sma_cross_strat import SMACross
from algo_trading.repositories.db_repository import DBRepository
from algo_trading.repositories.key_val_repository import KeyValueRepository
from algo_trading.repositories.obj_store_repository import ObjStoreRepository
from algo_trading.config.controllers import (
    ColumnController,
    StockStatusController,
    ObjStoreController,
    DBHandlerController,
    KeyValueController,
)
from algo_trading.utils.utils import str_to_dt, dt_to_str
from algo_trading.config import DB_INFO, KV_INFO, OBJ_STORE_INFO, CONFIG


LOG, LOG_INFO = get_main_logger(
    log_name="sma_cross_dag",
    file_name=os.path.join("logs", f"sma_cross_dag_{dt_to_str(datetime.today())}.log"),
    log_level=LogLevelController.info,
)

DB_HANDLER = DBRepository(
    DB_INFO,
    DBHandlerController[CONFIG.db_repo],
    LOG_INFO,
).handler

KV_HANDLER = KeyValueRepository(
    KV_INFO,
    KeyValueController[CONFIG.kv_repo],
    LOG_INFO,
).handler

OBJ_STORE_HANDLER = ObjStoreRepository(
    OBJ_STORE_INFO,
    ObjStoreController[CONFIG.obj_store_repo],
    LOG_INFO,
).handler


def backfill_redis(new_ticker_paths: Dict[str, Dict[str, str]]) -> None:
    """Gets up to date redis data for new tickers to indicate last
    cross dates and status.

    Args:
        new_ticker_paths (Dict): New tickers and paths to download data from.
    """

    for ticker, path_dict in new_ticker_paths.items():
        data = OBJ_STORE_HANDLER.download_fileobj(
            bucket=path_dict["bucket"],
            key=path_dict["key"],
            file_type="df",
        )
        cross_info = KV_HANDLER.get(ticker)
        if cross_info is not None:
            LOG.info(
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

        KV_HANDLER.set(ticker, cross_info)
        LOG.info(f"Updated Redis for {ticker}: {json.dumps(cross_info, indent=2)}")


def update_redis(tickers: List, new_tickers: List) -> None:
    """
    Updates redis information for existing tickers. Includes
    last cross up date and last cross down date.

    Args:
        tickers (List): All tickers
        new_tickers (List): New tickers. Used for exclusion.

    Raises:
        ValueError: Error raised if no redis data for the ticker.
    """

    for ticker in [t for t in tickers if t not in new_tickers]:
        data = DB_HANDLER.get_days_back(ticker, 2)
        cross_info = KV_HANDLER.get(ticker)

        if cross_info is None:
            raise ValueError(f"No Redis data for ticker {ticker}...")

        cross_info = json.loads(cross_info)
        LOG.info(f"Current Redis for {ticker}: {json.dumps(cross_info, indent=2)}")

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

        KV_HANDLER.set(ticker, cross_info)
        LOG.info(f"Updated Redis for {ticker}: {json.dumps(cross_info, indent=2)}")


if __name__ == "__main__":
    pass

    # Loading in and parsing config
    # config_path = os.path.join(
    #     os.path.dirname(os.path.realpath(__file__)).replace("dags", "algo_trading"),
    #     "config/config.yml",
    # )
    # config = yl.safe_load(open(config_path, "r"))
    # tickers = config["ticker_list"]
    # db_handler = config["db_repo"]
    # data_handler = config["data_repo"]
    # kv_handler = config["kv_repo"]

    # LOG.info("testing from sma")

    # backfill_redis(new_ticker_data)

    # update_redis(CONFIG.ticker_list, new_tickers)
