from datetime import datetime
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
from algo_trading.utils.utils import dt_to_str, str_to_dt


class SMACrossUtils:
    @staticmethod
    def check_cross_up(
        data: pd.DataFrame,
        index: int,
        cross_info: SMACrossInfo,
    ) -> SMACrossInfo:
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
            cross_info (SMACrossInfo): Current cross info to analyze.

        Returns:
            SMACrossInfo: Updated (or not) SMACrossInfo object.
        """
        if (
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
        ):
            cross_info.last_cross_up = dt_to_str(
                data[ColumnController.date.value].iloc[index]
            )

        return cross_info

    @staticmethod
    def check_cross_down(
        data: pd.DataFrame,
        index: int,
        cross_info: SMACrossInfo,
    ) -> SMACrossInfo:
        """
        Checks to see if a cross down occured by looking at
        the current date 7 and 21 day SMA and the previous date
        7 and 21 day SMA.

        **DEPENDENT ON CHECK_CROSS_UP**
        Because we double check if a cross up occured below the sma_50
        so it wasn't considered/logged. In that case, the last_cross_up
        would still be less than last_cross_down. So if that is the case,
        we also ignore the cross down following the unlogged cross up.

        **NOTE**
        We expect the dataframe to be in ASCENDING order by date.

        Args:
            data (pd.DataFrame): Data to parse SMA info.
            index (int): Indicates current day. index + 1 = prev day.
            cross_info (SMACrossInfo): Cross info to update.

        Returns:
            SMACrossInfo: Updated SMACross info object.
        """
        if (
            (
                data[ColumnController.ma_7.value].iloc[index]
                < data[ColumnController.ma_21.value].iloc[index]
            )
            and (
                data[ColumnController.ma_7.value].iloc[index - 1]
                >= data[ColumnController.ma_21.value].iloc[index - 1]
            )
            and (
                str_to_dt(cross_info.last_cross_down)
                < str_to_dt(cross_info.last_cross_up)
            )
        ):
            cross_info.last_cross_down = dt_to_str(
                data[ColumnController.date.value].iloc[index]
            )

        return cross_info


class SMACross(AbstractStrategy):
    def __init__(
        self,
        ticker: str,
        cross_db: AbstractKeyValueRepository,
        date: datetime,
    ) -> None:

        self.ticker = ticker
        self.cross_db = cross_db
        self.date = date

    @property
    def cross_info(self) -> SMACrossInfo:
        try:
            return self._cross_info
        except AttributeError:
            self._cross_info = SMACrossInfo(
                **json.loads(self.cross_db.get(self.ticker))
            )
            return self._cross_info

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

        return TradeEvent(date=self.date, ticker=self.ticker, signal=signal)
