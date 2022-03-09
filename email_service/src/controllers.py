from enum import Enum
from typing import Optional
from pydantic import BaseModel
from datetime import datetime

from algo_trading.utils.utils import dt_to_str


class Signal(str, Enum):
    buy = "BUY"
    sell = "SELL"


class Strategy(str, Enum):
    sma_cross = "SMACross"


class NotificationPayload(BaseModel):
    signal: Signal
    strategy: Strategy
    date: str = dt_to_str(datetime.today())
    test: bool = False
