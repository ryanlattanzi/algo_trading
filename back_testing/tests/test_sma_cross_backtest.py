import json
import pandas as pd
import numpy as np

from algo_trading.config.controllers import (
    ColumnController,
    DBHandlerController,
    SMACrossInfo,
    StockStatusController,
)
from algo_trading.logger.default_logger import get_main_logger
from algo_trading.logger.controllers import LogLevelController
from algo_trading.utils.utils import str_to_dt

from back_testing.src.controllers import (
    BackTestOptions,
    BackTestPayload,
    BackTestResult,
)
from back_testing.src.sma_cross_backtest import SMACrossBackTester

"""
Functions tested in this module:
- SMACrossBackTester._init_fake_key_value()
- SMACrossBackTester._get_num_shares()
- SMACrossBackTester._get_new_capital()
- SMACrossBackTester._get_percent_change()
- SMACrossBackTester.test()
"""

DATA = pd.read_csv("./sample_data/backtest_sample_data.csv")
DATA[ColumnController.date.value] = pd.to_datetime(
    DATA[ColumnController.date.value],
)

DB_INFO = {"data": DATA}
DB_HANDLER = DBHandlerController.fake
LOG, LOG_INFO = get_main_logger(
    log_name="test_SMA_backtest",
    file_name=None,
    log_level=LogLevelController.debug,
)


class TestSMACrossBackTester:
    """
    Tests all methods in SMACrossBackTester
    """

    ticker = "AAPL"
    payload = BackTestPayload(
        ticker=ticker,
        strategy=BackTestOptions.sma_cross,
    )
    tester = SMACrossBackTester(
        payload=payload,
        db_info=DB_INFO,
        db_handler=DB_HANDLER,
        log=LOG,
        log_info=LOG_INFO,
    )

    def test_check_price_data(self):
        """
        Double check price data is what we
        expect.
        """
        assert self.tester.price_data.equals(DATA)

    def test_init_fake_key_value_init_sell(self):
        """
        Ensures we initialize the fake key value
        data structure appropriately with a sell signal.
        """
        last_status, cross_info = self.tester._init_fake_key_value()
        assert last_status == StockStatusController.sell
        assert cross_info == {
            self.ticker: json.dumps(
                SMACrossInfo(
                    last_cross_up="2005-01-01",
                    last_cross_down="2005-01-02",
                    last_status=StockStatusController.sell,
                ).dict()
            )
        }

    def test_init_fake_key_value_init_buy(self):
        """
        Ensures we initialize the fake key value
        data structure appropriately with a buy signal.
        Had to create a new instance of the SMACrossBackTester
        with a data slice that gave use this result.
        """
        init_buy_tester = SMACrossBackTester(
            payload=self.payload,
            db_info={"data": DATA[4:10]},
            db_handler=DB_HANDLER,
            log=LOG,
            log_info=LOG_INFO,
        )
        last_status, cross_info = init_buy_tester._init_fake_key_value()
        assert last_status == StockStatusController.buy
        assert cross_info == {
            self.ticker: json.dumps(
                SMACrossInfo(
                    last_cross_up="2005-01-06",
                    last_cross_down="2005-01-05",
                    last_status=StockStatusController.buy,
                ).dict()
            )
        }

    def test_init_fake_key_value_empty_ma_values(self):
        """
        If we are considering a stock from the beginning of
        time, it won't have sma_7 or sma_21 values. We
        want to initialize it with a sell signal since we
        are waiting for that first buy.
        """
        init_row = pd.DataFrame(
            [
                {
                    "date": str_to_dt("2005-01-02"),
                    "open": "",
                    "high": "",
                    "low": "",
                    "close": "",
                    "adj_close": "",
                    "volume": "",
                    "ma_200": np.nan,
                    "ma_50": np.nan,
                    "ma_21": np.nan,
                    "ma_7": np.nan,
                }
            ]
        )
        init_buy_tester = SMACrossBackTester(
            payload=self.payload,
            db_info={"data": pd.concat([init_row, DATA]).reset_index(drop=True)},
            db_handler=DB_HANDLER,
            log=LOG,
            log_info=LOG_INFO,
        )
        last_status, cross_info = init_buy_tester._init_fake_key_value()
        assert last_status == StockStatusController.sell
        assert cross_info == {
            self.ticker: json.dumps(
                SMACrossInfo(
                    last_cross_up="2004-12-31",
                    last_cross_down="2005-01-01",
                    last_status=StockStatusController.sell,
                ).dict()
            )
        }

    def test_run_function(self):
        """
        Testing the run function.
        """
        test_result, trade_book = self.tester.test()
        assert test_result == BackTestResult(
            **{
                "ticker": "AAPL",
                "start_date": "2005-01-03",
                "end_date": "2010-12-31",
                "init_cap": 1000.0,
                "final_cap": 3380.4113608601306,
                "cap_gains": 238.04,
                "num_trades": 59,
            }
        )

        assert trade_book == {
            "2005-01-07": "BUY",
            "2005-03-09": "SELL",
            "2005-03-24": "BUY",
            "2005-04-14": "SELL",
            "2005-05-23": "BUY",
            "2005-06-10": "SELL",
            "2005-06-24": "BUY",
            "2005-07-01": "SELL",
            "2005-07-11": "BUY",
            "2005-10-12": "SELL",
            "2005-10-19": "BUY",
            "2006-01-26": "SELL",
            "2006-04-06": "BUY",
            "2006-05-17": "SELL",
            "2006-07-25": "BUY",
            "2006-10-10": "SELL",
            "2006-10-19": "BUY",
            "2006-12-13": "SELL",
            "2007-01-10": "BUY",
            "2007-01-26": "SELL",
            "2007-02-21": "BUY",
            "2007-03-07": "SELL",
            "2007-03-08": "BUY",
            "2007-04-16": "SELL",
            "2007-04-26": "BUY",
            "2007-07-02": "SELL",
            "2007-07-03": "BUY",
            "2007-08-06": "SELL",
            "2007-11-30": "BUY",
            "2008-01-08": "SELL",
            "2008-06-03": "BUY",
            "2008-06-11": "SELL",
            "2008-08-12": "BUY",
            "2008-09-03": "SELL",
            "2009-01-29": "BUY",
            "2009-02-23": "SELL",
            "2009-03-16": "BUY",
            "2009-05-15": "SELL",
            "2009-05-27": "BUY",
            "2009-06-22": "SELL",
            "2009-07-01": "BUY",
            "2009-07-13": "SELL",
            "2009-07-14": "BUY",
            "2009-11-03": "SELL",
            "2009-11-11": "BUY",
            "2009-12-03": "SELL",
            "2009-12-24": "BUY",
            "2010-01-22": "SELL",
            "2010-02-18": "BUY",
            "2010-05-10": "SELL",
            "2010-06-02": "BUY",
            "2010-06-14": "SELL",
            "2010-06-16": "BUY",
            "2010-07-02": "SELL",
            "2010-07-27": "BUY",
            "2010-08-13": "SELL",
            "2010-09-08": "BUY",
            "2010-11-17": "SELL",
            "2010-11-29": "BUY",
        }
