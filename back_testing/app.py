from typing import Dict
from fastapi import FastAPI

from algo_trading.logger.default_logger import get_main_logger
from algo_trading.logger.controllers import LogLevelController

from src.sma_cross_backtest import SMACrossBackTester
from src.controllers import BackTestPayload, BackTestOptions
from src.config import DB_INFO, DB_HANDLER

app = FastAPI()

LOG, LOG_INFO = get_main_logger(
    log_name="SMA_backtest",
    file_name=None,
    log_level=LogLevelController.info,
)


@app.post("/backtest")
def backtest(payload: BackTestPayload) -> Dict:
    """
    Runs the a backtester for the given strategy,
    ticker, and start date.

    Args:
        payload (BackTestPayload): Pydantic payload model.

    Returns:
        Dict: Status
    """

    strategies = {
        BackTestOptions.sma_cross: SMACrossBackTester(
            payload=payload,
            db_info=DB_INFO,
            db_handler=DB_HANDLER,
            log=LOG,
            log_info=LOG_INFO,
        ),
    }

    tester = strategies[payload.strategy]
    res, _ = tester.test()

    return {
        "status": 200,
        "body": res,
    }
