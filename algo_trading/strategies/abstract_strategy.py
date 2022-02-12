from abc import ABC, abstractmethod
from algo_trading.config.events import TradeEvent


class AbstractStrategy(ABC):
    @abstractmethod
    def run(self) -> TradeEvent:
        pass
