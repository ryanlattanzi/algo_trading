from typing import Dict
from fastapi import FastAPI

from src.sma_cross_backtest import SMACrossBackTester
from src.controllers import BackTestPayload, BackTestOptions, BackTestResult

app = FastAPI()

STRATEGIES = {
    BackTestOptions.sma_cross: SMACrossBackTester,
}


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

    tester = STRATEGIES[payload.strategy](payload=payload)
    res: BackTestResult = tester.test()

    return {
        "status": 200,
        "body": res,
    }
