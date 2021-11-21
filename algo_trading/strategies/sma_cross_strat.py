from typing import Tuple, Dict
import json
from datetime import datetime

from algo_trading.strategies.abstract_strategy import AbstractStrategy
from algo_trading.strategies.events import TradeEvent
from algo_trading.config.controllers import ColumnController, StockStatusController
from algo_trading.repositories.db_repository import DBRepository
from algo_trading.repositories.key_val_repository import KeyValueRepository
from algo_trading.utils.utils import str_to_dt


class SMACross(AbstractStrategy):
    def __init__(
        self,
        ticker: str,
        sma_db: DBRepository,
        cross_db: KeyValueRepository,
    ) -> None:

        self.ticker = ticker
        self.sma_db = sma_db.handler
        self.cross_db = cross_db.handler

    @property
    def cross_info(self) -> Dict:
        return json.loads(self.cross_db.get(self.ticker))

    @property
    def sma_info(self) -> Dict:
        data = self.sma_db.get_days_back(self.ticker, 1)
        try:
            data = data.to_dict("records")[0]
        except IndexError:
            data = {ColumnController.date.value: None}
        return data

    def _update_last_status(self, signal: StockStatusController) -> None:
        current = self.cross_info
        current[ColumnController.last_status.value] = signal.value
        self.cross_db.set(self.ticker, current)

    def run(self) -> Tuple[datetime.date, str, str]:
        last_cross_up_int = str_to_dt(
            self.cross_info[ColumnController.last_cross_up.value]
        )
        last_cross_down_int = str_to_dt(
            self.cross_info[ColumnController.last_cross_down.value]
        )
        last_status = self.cross_info[ColumnController.last_status.value]

        date = self.sma_info[ColumnController.date.value]

        if date:
            if last_cross_up_int > last_cross_down_int:
                if last_status == StockStatusController.buy.value:
                    signal = StockStatusController.hold
                # elif last_status == StockStatusController.hold.value:
                #     signal = StockStatusController.hold
                # elif last_status == StockStatusController.wait.value:
                #     signal = StockStatusController.buy
                elif last_status == StockStatusController.sell.value:
                    signal = StockStatusController.buy
                    self._update_last_status(signal)
            else:
                if last_status == StockStatusController.buy.value:
                    signal = StockStatusController.sell
                    self._update_last_status(signal)
                # elif last_status == StockStatusController.hold.value:
                #     signal = StockStatusController.sell
                # elif last_status == StockStatusController.wait.value:
                #     signal = StockStatusController.wait
                elif last_status == StockStatusController.sell.value:
                    signal = StockStatusController.wait

        else:
            signal = StockStatusController.wait

        return TradeEvent(date=date, ticker=self.ticker, signal=signal)
