from typing import Union
from pydantic import BaseModel
import datetime

from algo_trading.config.controllers import StockStatusController


class TradeEvent(BaseModel):

    date: Union[datetime.date, None]
    ticker: str
    signal: StockStatusController


class BackTestResult(BaseModel):

    ticker: str
    start_date: str
    end_date: str
    init_cap: float
    final_cap: float
    cap_gains: float
    num_trades: int
