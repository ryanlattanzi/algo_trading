from abc import ABC, abstractmethod
from algo_trading.strategies.events import TradeEvent


class AbstractStrategy(ABC):
    @abstractmethod
    def run(self) -> TradeEvent:
        pass
