from typing import Dict
import json
import pandas as pd

from algo_trading.strategies.abstract_strategy import AbstractStrategy
from algo_trading.config.events import TradeEvent
from algo_trading.config.controllers import (
    ColumnController,
    StockStatusController,
    SMACrossInfo,
)
from algo_trading.repositories.db_repository import AbstractDBRepository
from algo_trading.repositories.key_val_repository import AbstractKeyValueRepository
from algo_trading.utils.utils import str_to_dt


class SMACross(AbstractStrategy):
    def __init__(
        self,
        ticker: str,
        sma_db: AbstractDBRepository,
        cross_db: AbstractKeyValueRepository,
    ) -> None:

        self.ticker = ticker
        self.sma_db = sma_db
        self.cross_db = cross_db

    @property
    def cross_info(self) -> SMACrossInfo:
        try:
            return self._cross_info
        except AttributeError:
            self._cross_info = SMACrossInfo(
                **json.loads(self.cross_db.get(self.ticker))
            )
            return self._cross_info

    @property
    def sma_info(self) -> Dict:
        data = self.sma_db.get_days_back(self.ticker, 1)
        try:
            data = data.to_dict("records")[0]
        except IndexError:
            data = {ColumnController.date.value: None}
        return data

    @staticmethod
    def cross_up(data: pd.DataFrame, index: int) -> bool:
        """Checks to see if a cross up occured by looking at
        the current date 7 and 21 day SMA and the previous date
        7 and 21 day SMA. Finally, we only consider cross up
        when the close price > 50 ay SMA otherwise the market
        is considered bearish.

        **NOTE**
        We expect the dataframe to be in ASCENDING order by date.

        Args:
            data (pd.DataFrame): Data to parse SMA info.
            index (int): Indicates current day. index + 1 = prev day.

        Returns:
            bool: True if all conditions are met.
        """
        return (
            (
                data[ColumnController.ma_7.value].iloc[index]
                >= data[ColumnController.ma_21.value].iloc[index]
            )
            and (
                data[ColumnController.ma_7.value].iloc[index - 1]
                < data[ColumnController.ma_21.value].iloc[index - 1]
            )
            and (
                data[ColumnController.close.value].iloc[index]
                > data[ColumnController.ma_50.value].iloc[index]
            )
        )

    @staticmethod
    def cross_down(data: pd.DataFrame, index: int) -> bool:
        """Checks to see if a cross down occured by looking at
        the current date 7 and 21 day SMA and the previous date
        7 and 21 day SMA.

        **NOTE**
        We expect the dataframe to be in ASCENDING order by date.

        Args:
            data (pd.DataFrame): Data to parse SMA info.
            index (int): Indicates current day. index + 1 = prev day.

        Returns:
            bool: True if all conditions are met.
        """
        return (
            data[ColumnController.ma_7.value].iloc[index]
            < data[ColumnController.ma_21.value].iloc[index]
        ) and (
            data[ColumnController.ma_7.value].iloc[index - 1]
            >= data[ColumnController.ma_21.value].iloc[index - 1]
        )

    def _update_last_status(self, signal: StockStatusController) -> None:
        """Updates the ast_status value to the given signal for the
        Key Value store.

        Args:
            signal (StockStatusController): Enumeration signal.
        """
        current = self.cross_info
        current.last_status = signal
        self.cross_db.set(self.ticker, current.dict())

    def run(self) -> TradeEvent:
        """Runs the SMACross strategy logic based on the last cross up/down
        in the Key Value store.

        Returns:
            TradeEvent: Event
        """
        last_cross_up = str_to_dt(self.cross_info.last_cross_up)
        last_cross_down = str_to_dt(self.cross_info.last_cross_down)
        last_status = self.cross_info.last_status

        date = self.sma_info[ColumnController.date.value]

        if date:
            if last_cross_up > last_cross_down:
                if last_status == StockStatusController.buy:
                    signal = StockStatusController.hold
                elif last_status == StockStatusController.sell:
                    signal = StockStatusController.buy
                    self._update_last_status(signal)
            else:
                if last_status == StockStatusController.buy:
                    signal = StockStatusController.sell
                    self._update_last_status(signal)
                elif last_status == StockStatusController.sell:
                    signal = StockStatusController.wait

        else:
            signal = StockStatusController.wait

        return TradeEvent(date=date, ticker=self.ticker, signal=signal)
