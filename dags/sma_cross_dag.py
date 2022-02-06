import os
import json
from typing import Dict, List
from datetime import datetime, timedelta

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
from algo_trading.config import (
    DB_INFO,
    KV_INFO,
    OBJ_STORE_INFO,
    CONFIG,
    LOG_BUCKET,
    LOG_KEY,
)


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

DEFAULT_START_DATE = "1900-01-02"


def backfill_redis(new_tickers: List[str]) -> None:
    """Gets up to date redis data for new tickers to indicate last
    cross dates and status.

    Args:
        new_tickers (List[str]): New tickers to backfill.
    """

    for ticker in new_tickers:
        data = DB_HANDLER.get_all(ticker)
        cross_info = KV_HANDLER.get(ticker)

        if cross_info is not None:
            LOG.info(f"Redis data for {ticker} already exists bum!")
            continue

        LOG.info(f"Backfilling cross up/down info for {ticker}")

        cross_info = {
            ColumnController.last_cross_up.value: None,
            ColumnController.last_cross_down.value: None,
            ColumnController.last_status.value: None,
        }

        # Dirty way of finding out last cross up and cross down - can def do better
        for i in range(len(data.index) - 1, 1, -1):
            if SMACross.cross_up(data, i):
                cross_info[ColumnController.last_cross_up.value] = dt_to_str(
                    data[ColumnController.date.value].iloc[i]
                )
                LOG.info(
                    f"Last cross up: {cross_info[ColumnController.last_cross_up.value]}"
                )
                break

        for i in range(len(data.index) - 1, 1, -1):
            if SMACross.cross_down(data, i):
                cross_info[ColumnController.last_cross_down.value] = dt_to_str(
                    data[ColumnController.date.value].iloc[i]
                )
                LOG.info(
                    f"Last cross down: {cross_info[ColumnController.last_cross_down.value]}"
                )
                break

        # Handling various cases if no cross_ups or cross_downs have occured yet.
        if (
            cross_info[ColumnController.last_cross_up.value]
            and not cross_info[ColumnController.last_cross_down.value]
        ):
            cross_info[ColumnController.last_cross_down.value] = dt_to_str(
                str_to_dt(cross_info[ColumnController.last_cross_up.value])
                - timedelta(days=1)
            )
            cross_info[
                ColumnController.last_status.value
            ] = StockStatusController.buy.value
        elif (
            cross_info[ColumnController.last_cross_down.value]
            and not cross_info[ColumnController.last_cross_up.value]
        ):
            cross_info[ColumnController.last_cross_up.value] = dt_to_str(
                str_to_dt(cross_info[ColumnController.last_cross_down.value])
                - timedelta(days=1)
            )
            cross_info[
                ColumnController.last_status.value
            ] = StockStatusController.sell.value
        elif (
            not cross_info[ColumnController.last_cross_down.value]
            and not cross_info[ColumnController.last_cross_up.value]
        ):
            cross_info[ColumnController.last_cross_down.value] = DEFAULT_START_DATE
            cross_info[ColumnController.last_cross_up.value] = dt_to_str(
                str_to_dt(DEFAULT_START_DATE) - timedelta(days=1)
            )
            cross_info[
                ColumnController.last_status.value
            ] = StockStatusController.sell.value
        else:
            if str_to_dt(
                cross_info[ColumnController.last_cross_down.value]
            ) < str_to_dt(cross_info[ColumnController.last_cross_up.value]):
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
            LOG.error(f"No Redis data for ticker {ticker}...")
            continue

        cross_info = json.loads(cross_info)

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


def finish_log() -> None:
    LOG.info(f"END OF SMA CROSS DAG ON {dt_to_str(datetime.today())}.\n")


def persist_log() -> None:
    OBJ_STORE_HANDLER.upload_file(
        LOG_INFO.file_name,
        LOG_BUCKET,
        LOG_KEY.format(
            log_name=LOG_INFO.log_name, run_date=dt_to_str(datetime.today())
        ),
    )
