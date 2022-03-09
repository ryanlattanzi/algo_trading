from typing import Union
from pydantic import BaseModel
import datetime

from algo_trading.config.controllers import StockStatusController


class TradeEvent(BaseModel):

    date: Union[datetime.date, None]
    ticker: str
    signal: StockStatusController
