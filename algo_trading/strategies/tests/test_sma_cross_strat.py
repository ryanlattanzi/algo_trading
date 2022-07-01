import json
from datetime import datetime
import pandas as pd

from algo_trading.config.controllers import (
    ColumnController,
    KeyValueController,
    StrategyInfo,
    StockStatusController,
)
from algo_trading.config.events import TradeEvent
from algo_trading.repositories.key_val_repository import KeyValueRepository
from algo_trading.strategies.sma_cross_strat import SMACrossUtils, SMACross


"""
Functions tested in this module:
- SMACrossUtils.check_cross_up()
- SMACrossUtils.check_cross_down()
- SMACross.run()
"""

DATA = pd.read_csv("./sample_data/strategy_sample_data.csv")
DATA[ColumnController.date.value] = pd.to_datetime(
    DATA[ColumnController.date.value],
)


class TestCheckCrossUp:
    """
    Tests basic functionality to register a
    fabricated cross up.
    """

    def test_cross_up(self):
        """
        From 2005-01-06 to 2005-01-07, index 4,
        we should have a cross up.
        """
        cross_info: StrategyInfo = SMACrossUtils.check_cross_up(
            DATA,
            4,
            StrategyInfo(),
        )
        assert cross_info.sma_last_cross_up == "2005-01-07"

    def test_no_cross_up(self):
        """
        Should be no cross up so last_cross_up stays the same
        as default values.
        """
        cross_info: StrategyInfo = SMACrossUtils.check_cross_up(
            DATA,
            5,
            StrategyInfo(),
        )
        assert cross_info.sma_last_cross_up == "1900-01-01"

    def test_cross_up_bear_market(self):
        """
        Even though there is a cross up from 01-04 to 01-05,
        if it is in a bear market, we should not log it. In this
        case, the close price has been set to be less than the 50
        day moving average a.k.a. a bear market. Hence, the
        last_cross_up value should not change and match the default.
        """
        cross_info: StrategyInfo = SMACrossUtils.check_cross_up(
            DATA,
            2,
            StrategyInfo(),
        )
        assert cross_info.sma_last_cross_up == "1900-01-01"


class TestCheckCrossDown:
    """
    Tests basic functionality to register a
    fabricated cross down.
    """

    def test_cross_down(self):
        """
        From 2005-01-03 to 2005-01-04, index 1,
        we should have a cross down.
        """
        init_cross_info = StrategyInfo(
            sma_last_cross_down="1899-12-31",
            sma_last_status=StockStatusController.buy,
        )
        cross_info: StrategyInfo = SMACrossUtils.check_cross_down(
            DATA,
            1,
            init_cross_info,
        )
        assert cross_info.sma_last_cross_down == "2005-01-04"

    def test_no_cross_down(self):
        """
        Should be no cross down so last_cross_down stays the same
        as default values.
        """
        cross_info: StrategyInfo = SMACrossUtils.check_cross_up(
            DATA,
            5,
            StrategyInfo(),
        )
        assert cross_info.sma_last_cross_down == "1900-01-02"

    def test_cross_down_bear_market(self):
        """
        This occurs when there was previously a cross up
        in a bear market so it shouldn't have been registered.
        We don't want to register a cross down in
        a bear market either.
        """
        cross_info: StrategyInfo = SMACrossUtils.check_cross_up(
            DATA,
            1,
            StrategyInfo(),
        )
        assert cross_info.sma_last_cross_down == "1900-01-02"


class TestSMACross:
    """
    Tests updating signals according to StrategyInfo
    data.
    """

    from algo_trading.logger.default_logger import get_main_logger
    from algo_trading.logger.controllers import LogLevelController

    # TODO: Make log_info optional for repositories...
    LOG, LOG_INFO = get_main_logger(
        log_name="DummyLog",
        file_name=None,
        log_level=LogLevelController.info,
    )

    TICKER = "DUMMY"
    TEST_DATE = datetime(1776, 7, 4)

    def test_sell_signal(self):
        """
        We should see a sell signal based off StrategyInfo values.
        Simulates having registered a cross_down recently (using the
        SMAUtils functions above), so we need to update the kv_repo
        to a sell signal and return the TradeEvent.
        """

        init_cross_info = StrategyInfo(
            sma_last_cross_up="1900-01-01",
            sma_last_cross_down="1900-01-02",
            sma_last_status=StockStatusController.buy,
        )
        init_kv = {self.TICKER: json.dumps(init_cross_info.dict())}
        fake_kv_repo = KeyValueRepository(
            kv_info=init_kv,
            kv_handler=KeyValueController.fake,
            log_info=self.LOG_INFO,
        ).handler

        sma = SMACross(self.TICKER, fake_kv_repo, date=self.TEST_DATE)
        result: TradeEvent = sma.run()

        assert result == TradeEvent(
            date=self.TEST_DATE,
            ticker=self.TICKER,
            signal=StockStatusController.sell,
        )

        assert StrategyInfo(
            **json.loads(fake_kv_repo.get(self.TICKER))
        ) == StrategyInfo(
            sma_last_cross_up="1900-01-01",
            sma_last_cross_down="1900-01-02",
            sma_last_status=StockStatusController.sell,
        )

    def test_buy_signal(self):
        """
        Returning a buy signal means we recently registered
        a cross up so need to update our kv_repo to a sell and
        return the appropriate TradeEvent.
        """

        init_cross_info = StrategyInfo(
            sma_last_cross_up="1900-01-01",
            sma_last_cross_down="1899-12-31",
            sma_last_status=StockStatusController.sell,
        )
        init_kv = {self.TICKER: json.dumps(init_cross_info.dict())}
        fake_kv_repo = KeyValueRepository(
            kv_info=init_kv,
            kv_handler=KeyValueController.fake,
            log_info=self.LOG_INFO,
        ).handler

        sma = SMACross(self.TICKER, fake_kv_repo, date=self.TEST_DATE)
        result: TradeEvent = sma.run()

        assert result == TradeEvent(
            date=self.TEST_DATE,
            ticker=self.TICKER,
            signal=StockStatusController.buy,
        )

        assert StrategyInfo(
            **json.loads(fake_kv_repo.get(self.TICKER))
        ) == StrategyInfo(
            sma_last_cross_up="1900-01-01",
            sma_last_cross_down="1899-12-31",
            sma_last_status=StockStatusController.buy,
        )

    def test_wait_signal(self):
        """
        We return a wait signal when we already have a sell
        signal and have not registered a cross up. So, this
        means we want to wait until we find a cross up to buy.
        """

        init_cross_info = StrategyInfo(
            sma_last_cross_up="1900-01-01",
            sma_last_cross_down="1900-01-02",
            sma_last_status=StockStatusController.sell,
        )
        init_kv = {self.TICKER: json.dumps(init_cross_info.dict())}
        fake_kv_repo = KeyValueRepository(
            kv_info=init_kv,
            kv_handler=KeyValueController.fake,
            log_info=self.LOG_INFO,
        ).handler

        sma = SMACross(self.TICKER, fake_kv_repo, date=self.TEST_DATE)
        result: TradeEvent = sma.run()

        assert result == TradeEvent(
            date=self.TEST_DATE,
            ticker=self.TICKER,
            signal=StockStatusController.wait,
        )

        assert (
            StrategyInfo(**json.loads(fake_kv_repo.get(self.TICKER))) == init_cross_info
        )

    def test_hold_signal(self):
        """
        We return a hold signal when we already have a buy
        signal and have not registered a cross down. So, this
        means we want to keep holding until we find a cross
        down to sell.
        """

        init_cross_info = StrategyInfo(
            sma_last_cross_up="1900-01-01",
            sma_last_cross_down="1899-12-31",
            sma_last_status=StockStatusController.buy,
        )
        init_kv = {self.TICKER: json.dumps(init_cross_info.dict())}
        fake_kv_repo = KeyValueRepository(
            kv_info=init_kv,
            kv_handler=KeyValueController.fake,
            log_info=self.LOG_INFO,
        ).handler

        sma = SMACross(self.TICKER, fake_kv_repo, date=self.TEST_DATE)
        result: TradeEvent = sma.run()

        assert result == TradeEvent(
            date=self.TEST_DATE,
            ticker=self.TICKER,
            signal=StockStatusController.hold,
        )

        assert (
            StrategyInfo(**json.loads(fake_kv_repo.get(self.TICKER))) == init_cross_info
        )
