from datetime import datetime
from typing import Dict
import json
import pandas as pd

from algo_trading.strategies.abstract_strategy import AbstractStrategy
from algo_trading.config.events import TradeEvent
from algo_trading.config.controllers import (
    ColumnController,
    StockStatusController,
    StrategyInfo,
)
from algo_trading.repositories.db_repository import AbstractDBRepository
from algo_trading.repositories.key_val_repository import AbstractKeyValueRepository
from algo_trading.utils.utils import dt_to_str, str_to_dt


class MACDCrossUtils:
    @staticmethod
    def _calc_macd_signal(
        data: pd.DataFrame,
        index: int,
    ) -> tuple:
        """
        This function will calculate the MACD line based on the 26EMA - 12EMA and subtract it from the 9EMA to find the
        convergence/divergence value for both the current and previous day. Signal Line is the 9EMA.

        Args:
            data (pd.DataFrame): Data to parse EMA info.
            index (int): Indicates current day. Index - 1 is previous day
        Returns:
            tuple: Provides the current day and previous day, respectively, MACD convergence/divergence value (macd_cdv).
        """
        # Calculates the MACD signal value of the current day and previous day
        macd_signal_curr = (
            data[ColumnController.ema_26.value].iloc[index]
            - data[ColumnController.ema_12.value].iloc[index]
        )
        macd_signal_prev = (
            data[ColumnController.ema_26.value].iloc[index - 1]
            - data[ColumnController.ema_12.value].iloc[index - 1]
        )

        # Calculates the convergence/divergence value for the current and previous day
        macd_cdv_curr = (
            macd_signal_curr - data[ColumnController.ema_9.value].iloc[index]
        )
        macd_cdv_prev = (
            macd_signal_prev - data[ColumnController.ema_9.value].iloc[index - 1]
        )

        return macd_cdv_curr, macd_cdv_prev

    @staticmethod
    def check_cross_up(
        data: pd.DataFrame,
        index: int,
        cross_info: StrategyInfo,
    ) -> StrategyInfo:
        """
        This function will check the macd current day and previous day to determine if a cross up event happened.

        Args:
            data (pd.DataFrame): dataframe from postgres with all stock data.
            index (int): index is the current day, index - 1 is the previous day.
            cross_info (StrategyInfo): current cross info stored in Redis for the ticker.

        Returns:
            StrategyInfo: Returns the date of when a cross up occured.
        """

        macd_cdv_curr, macd_cdv_prev = MACDCrossUtils._calc_macd_signal(data, index)

        # Checks to see if the cdv on the current day is greater than 0 (meaning that MACD line is greater than Signal)
        if macd_cdv_curr > 0 and macd_cdv_prev <= 0:
            cross_info.macd_last_cross_up = dt_to_str(
                data[ColumnController.date.value].iloc[index]
            )

        return cross_info

    @staticmethod
    def check_cross_down(
        data: pd.DataFrame,
        index: int,
        cross_info: StrategyInfo,
    ) -> StrategyInfo:
        """
        This function will check the macd current day and previous day to determine if a cross down event happened.

        Args:
            data (pd.DataFrame): dataframe from postgres with all stock data.
            index (int): index is the current day, index -1 is the previous day.
            cross_info (StrategyInfo): current cross info stored in Redis for the ticker.

        Returns:
            StrategyInfo: Returns the updated cross info to Redis with the new cross down event.
        """

        macd_cdv_curr, macd_cdv_prev = MACDCrossUtils._calc_macd_signal(data, index)

        # This Means that the MACD line goes below the Signal Line
        if macd_cdv_curr < 0 and macd_cdv_prev >= 0:
            cross_info.macd_last_cross_down = dt_to_str(
                data[ColumnController.date.value].iloc[index]
            )

        return cross_info


class MACDCross(AbstractStrategy):
    def __init__(
        self,
        ticker: str,
        macd_db: AbstractKeyValueRepository,
        date: datetime,
    ) -> None:
        self.ticker = ticker
        self.macd_db = macd_db
        self.date = date

    @property
    def cross_info(self) -> StrategyInfo:
        try:
            return self._cross_info
        except AttributeError:
            self._cross_info = StrategyInfo(**json.loads(self.macd_db.get(self.ticker)))
            return self._cross_info

    @property
    def macd_info(self) -> Dict:
        data = self.macd_db.get_days_back(self.ticker, 1)
        try:
            data = data.to_dict("records")[0]
        except IndexError:
            data = {ColumnController.date.value: None}
        return data

    def _update_last_status(self, signal: StockStatusController) -> None:
        """Updates the last_status value to the given signal for the
        Key Value store.

        Args:
            signal (StockStatusController): Enumeration signal.
        """
        current = self.cross_info
        current.macd_last_status = signal
        self.macd_db.set(self.ticker, current.dict())

    def run(self) -> TradeEvent:
        """Runs the MACDCross strategy logic based on the last cross up/down
        in the Key Value store.

        Returns:
            TradeEvent: Event
        """
        last_cross_up = str_to_dt(self.cross_info.macd_last_cross_up)
        last_cross_down = str_to_dt(self.cross_info.macd_last_cross_down)
        last_status = self.cross_info.macd_last_status

        # Added this to trouble shoot the macd_info function

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
