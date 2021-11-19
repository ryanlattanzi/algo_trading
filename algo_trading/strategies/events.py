from dataclasses import dataclass
import datetime

from algo_trading.config.controllers import StockStatusController


class Event:
    pass


@dataclass
class TradeEvent(Event):
    date: datetime.date
    ticker: str
    signal: StockStatusController
