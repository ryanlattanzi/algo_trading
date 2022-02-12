import os
import json
from typing import List
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
    SMACrossInfo,
)
from algo_trading.config.events import TradeEvent
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


def backfill_redis(new_tickers: List[str]) -> None:
    """Gets up to date redis data for new tickers to indicate last
    cross dates and status.

    Args:
        new_tickers (List[str]): New tickers to backfill.
    """

    for ticker in new_tickers:
        data = DB_HANDLER.get_all(ticker)

        if KV_HANDLER.get(ticker) is not None:
            LOG.info(f"Redis data for {ticker} already exists bum!")
            continue

        LOG.info(f"Backfilling cross up/down info for {ticker}")

        cross_info = SMACrossInfo()

        # Dirty way of finding out last cross up and cross down - can def do better
        for i in range(len(data.index) - 1, 1, -1):
            if SMACross.cross_up(data, i):
                cross_info.last_cross_up = dt_to_str(
                    data[ColumnController.date.value].iloc[i]
                )
                LOG.info(f"Last cross up: {cross_info.last_cross_up}")
                break

        for i in range(len(data.index) - 1, 1, -1):
            if SMACross.cross_down(data, i):
                cross_info.last_cross_down = dt_to_str(
                    data[ColumnController.date.value].iloc[i]
                )
                LOG.info(f"Last cross down: {cross_info.last_cross_down}")
                break

        if str_to_dt(cross_info.last_cross_down) < str_to_dt(cross_info.last_cross_up):
            cross_info.last_status = StockStatusController.buy
        else:
            cross_info.last_status = StockStatusController.sell

        KV_HANDLER.set(ticker, cross_info.dict())
        LOG.info(
            f"Updated Redis for {ticker}: {json.dumps(cross_info.dict(), indent=2)}"
        )


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

        cross_info = SMACrossInfo(**json.loads(cross_info))

        if SMACross.cross_up(data, 0):
            cross_info.last_cross_up = dt_to_str(
                data[ColumnController.date.value].iloc[0]
            )

        elif SMACross.cross_down(data, 0):
            # Checks the case when we had a cross up in bear market
            if str_to_dt(cross_info.last_cross_down) < str_to_dt(
                cross_info.last_cross_up
            ):
                cross_info.last_cross_down = dt_to_str(
                    data[ColumnController.date.value].iloc[0]
                )

        KV_HANDLER.set(ticker, cross_info.dict())
        LOG.info(
            f"Updated Redis for {ticker}: {json.dumps(cross_info.dict(), indent=2)}"
        )


def run_sma(tickers: List) -> List[TradeEvent]:
    """Runs the SMA strategy for the given tickers.

    Args:
        tickers (List): Tickers to analyze with SMA.

    Returns:
        List[TradeEvent]: TradeEvents based off of algorithm.
    """
    events = []
    for ticker in tickers:
        sma = SMACross(ticker, DB_HANDLER, KV_HANDLER)
        result = sma.run()
        if result.signal in [StockStatusController.buy, StockStatusController.sell]:
            events.append(result)
        LOG.info(f"{ticker} {result.signal.value} Event on {result.date}")
    return events


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
