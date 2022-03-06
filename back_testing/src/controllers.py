from enum import Enum
from pydantic import BaseModel
from datetime import datetime

from algo_trading.config.controllers import TestPeriodController
from algo_trading.utils.utils import dt_to_str


class BackTestOptions(str, Enum):
    sma_cross = "sma_cross"


class BackTestPayload(BaseModel):
    ticker: str
    strategy: BackTestOptions
    start_date: str = "max"
    end_date: str = dt_to_str(datetime.today())
    starting_capital: int = 1000


class BackTestResult(BaseModel):
    ticker: str
    start_date: str
    end_date: str
    init_cap: float
    final_cap: float
    cap_gains: float
    num_trades: int
