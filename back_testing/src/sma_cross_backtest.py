from datetime import timedelta
from logging import Logger
import json
from typing import Dict, Tuple
import pandas as pd
from pydantic import validate_arguments


from algo_trading.logger.controllers import LogConfig
from algo_trading.utils.utils import dt_to_str, str_to_dt
from algo_trading.strategies.sma_cross_strat import SMACross, SMACrossUtils
from algo_trading.repositories.db_repository import AbstractDBRepository, DBRepository
from algo_trading.repositories.key_val_repository import KeyValueRepository
from algo_trading.config.controllers import (
    ColumnController,
    DBHandlerController,
    KeyValueController,
    StockStatusController,
    SMACrossInfo,
)
from algo_trading.utils.calculations import Calculator

from .controllers import BackTestPayload, BackTestResult

# TODO: PERSIST LOGS TO MINIO BUCKET


class SMACrossBackTester:
    @validate_arguments(config=dict(arbitrary_types_allowed=True))
    def __init__(
        self,
        payload: BackTestPayload,
        db_info: Dict,
        db_handler: DBHandlerController,
        log: Logger,
        log_info: LogConfig,
    ) -> None:
        """
        SMACross Backtesting class. Entrypoint is the test() method.

        Args:
            payload (BackTestPayload): Pydantic paylod model.
            db_info (Dict[str, str], optional): DB to get price data. Defaults to DB_INFO.
            db_handler (DBHandlerController, optional): Defaults to DB_HANDLER.
            log (Logger, optional): Defaults to LOG.
            log_info (LogConfig, optional): Defaults to LOG_INFO.
        """
        self.ticker = payload.ticker
        self.start_date = payload.start_date
        self.end_date = payload.end_date
        self.starting_capital = payload.starting_capital

        self.db_info = db_info
        self.db_handler = db_handler
        self.log = log
        self.log_info = log_info

        self.fake_kv_repo = None

    @property
    def db_repo(self) -> AbstractDBRepository:
        """
        Live prices DB to pull data from to test.

        Returns:
            AbstractDBRepository: DB Repo handler object.
        """
        try:
            return self._db_repo
        except AttributeError:
            self._db_repo = DBRepository(
                self.db_info,
                self.db_handler,
                self.log_info,
            ).handler
            return self._db_repo

    @property
    def price_data(self) -> pd.DataFrame:
        """
        Pulling data from the real DB. This is why we needed db_info.
        Caches this data for testing so we don't have to clog the
        live DB.

        Returns:
            pd.DataFrame: Price DF to use for testing.
        """
        try:
            return self._price_data
        except AttributeError:
            if self.start_date == "max":
                self._price_data = self.db_repo.get_until_date(
                    self.ticker, self.end_date
                )
                self._price_data = Calculator.calculate_sma(
                    self._price_data, ColumnController.close.value
                )
            else:
                # Getting 199 days back in order to calculate SMA values
                # for the given start date

                row_nums = self.db_repo.get_row_num(self.ticker)
                start_date_idx = row_nums.index[
                    row_nums[ColumnController.date.value] == str_to_dt(self.start_date)
                ].tolist()[0]
                pull_date_idx = max(0, int(start_date_idx) - 199)
                pull_date = row_nums.iloc[pull_date_idx].to_dict()[
                    ColumnController.date.value
                ]
                self._price_data = self.db_repo.get_dates_between(
                    self.ticker, dt_to_str(pull_date), self.end_date
                )
                self._price_data = Calculator.calculate_sma(
                    self._price_data, ColumnController.close.value
                )
                self._price_data = self._price_data.iloc[
                    (start_date_idx - pull_date_idx) :
                ]
            return self._price_data

    def _init_fake_key_value(self) -> Tuple[StockStatusController, Dict[str, str]]:
        """
        Initializes the fake key,value store depending on the first day
        of the test data.

        Returns:
            Tuple[StockStatusController, Dict[str, str]]: Initialized status as a dict.
        """
        first_day = self.price_data.iloc[0].to_dict()
        if (
            first_day[ColumnController.ma_7.value]
            > first_day[ColumnController.ma_21.value]
        ):
            last_cross_up = dt_to_str(
                first_day[ColumnController.date.value] - timedelta(days=1)
            )
            last_cross_down = dt_to_str(
                first_day[ColumnController.date.value] - timedelta(days=2)
            )
            last_status = StockStatusController.buy
        else:
            last_cross_up = dt_to_str(
                first_day[ColumnController.date.value] - timedelta(days=2)
            )
            last_cross_down = dt_to_str(
                first_day[ColumnController.date.value] - timedelta(days=1)
            )
            last_status = StockStatusController.sell

        cross_info = SMACrossInfo(
            last_cross_up=last_cross_up,
            last_cross_down=last_cross_down,
            last_status=last_status,
        )

        return last_status, {self.ticker: json.dumps(cross_info.dict())}

    def _get_num_shares(self, cash: float, share_price: float) -> float:
        """
        Simple calculation of num shares to buy.

        Args:
            cash (float): Current cash
            share_price (float): Share price

        Returns:
            float: Num shares to purchase
        """
        return cash / share_price

    def _get_new_capital(self, num_shares: float, share_price: float) -> float:
        """
        Simple calculation of capitcal after selling.

        Args:
            num_shares (float): Num shares to sell
            share_price (float): Share price

        Returns:
            float: Liquid capital
        """
        return num_shares * share_price

    def _get_percent_change(
        self, start_cap: float, end_cap: float, precision: int = 2
    ) -> float:
        """
        Generate percent change from starting and ending capital.

        Args:
            start_cap (float): Starting capital
            end_cap (float): Ending capital
            precision (int, optional): Decimal places. Defaults to 2.

        Returns:
            float: Percent change
        """
        return round(((end_cap - start_cap) / start_cap) * 100, precision)

    def test(self) -> Tuple[BackTestResult, Dict[str, str]]:
        """
        Runs the SMA strategy over the cached price data.
        Instantiates a fake DBRepository and a fake KeyValueRepository
        to 'query' from.

        Returns:
            BackTestResult: Results of the back test.
            Dict[str, StockStatusController]: Trade book used for testing.
        """

        trade_book = {}

        init_status, init_kv = self._init_fake_key_value()
        fake_kv_repo = KeyValueRepository(
            kv_info=init_kv,
            kv_handler=KeyValueController.fake,
            log_info=self.log_info,
        ).handler

        starting_cap = self.starting_capital
        num_trades = 0
        if init_status == StockStatusController.buy:
            num_shares = self._get_num_shares(
                self.starting_capital,
                self.price_data[ColumnController.close.value].iloc[0],
            )
            init_message = f"{num_shares} shares"
        else:
            num_shares = 0
            init_message = f"${self.starting_capital}"

        self.log.info(
            f"Beginning SMA Cross strategy with {init_message} for "
            + f"{self.ticker.upper()} at price "
            + f"{self.price_data[ColumnController.close.value].iloc[0]} on "
            + f"{self.price_data[ColumnController.date.value].iloc[0]}"
        )

        for idx, row in self.price_data.iterrows():
            if idx == 0:
                # Skip the first day.
                continue
            else:

                # Current key/val store for self.ticker
                cross_info = SMACrossInfo(**json.loads(fake_kv_repo.get(self.ticker)))

                cross_info = SMACrossUtils.check_cross_up(
                    self.price_data[: (idx + 1)],
                    idx,
                    cross_info,
                )

                cross_info = SMACrossUtils.check_cross_down(
                    self.price_data[: (idx + 1)],
                    idx,
                    cross_info,
                )

                fake_kv_repo.set(self.ticker, cross_info.dict())
                date = self.price_data.iloc[idx][ColumnController.date.value]
                sma = SMACross(self.ticker, fake_kv_repo, date)
                result = sma.run()
                if result.signal == StockStatusController.buy:
                    num_shares = self._get_num_shares(
                        self.starting_capital, row[ColumnController.close.value]
                    )
                    num_trades += 1
                    self.log.debug(
                        f"Bought {num_shares} shares at price {row[ColumnController.close.value]} "
                        + f"on {row[ColumnController.date.value]}."
                    )
                    trade_book[
                        dt_to_str(row[ColumnController.date.value])
                    ] = StockStatusController.buy.value
                elif result.signal == StockStatusController.sell:
                    self.starting_capital = self._get_new_capital(
                        num_shares, row[ColumnController.close.value]
                    )
                    num_trades += 1
                    self.log.debug(
                        f"Sold {num_shares} shares at price "
                        + f"{row[ColumnController.close.value]} on "
                        + f"{row[ColumnController.date.value]} for a new capital of {self.starting_capital}."
                    )
                    trade_book[
                        dt_to_str(row[ColumnController.date.value])
                    ] = StockStatusController.sell.value
                    num_shares = 0

        if num_shares != 0:
            self.starting_capital = self._get_new_capital(
                num_shares, self.price_data[ColumnController.close.value].iloc[-1]
            )
            self.log.debug(
                f"Finished with {num_shares} shares at price "
                + f"{self.price_data[ColumnController.close.value].iloc[-1]} "
                + f"on {self.price_data[ColumnController.date.value].iloc[-1]}."
            )
            self.log.debug(f"Selling all for a new capital of {self.starting_capital}.")
        percent_change = self._get_percent_change(starting_cap, self.starting_capital)

        res = BackTestResult(
            ticker=self.ticker,
            start_date=dt_to_str(self.price_data[ColumnController.date.value].iloc[0]),
            end_date=dt_to_str(self.price_data[ColumnController.date.value].iloc[-1]),
            init_cap=starting_cap,
            final_cap=self.starting_capital,
            cap_gains=percent_change,
            num_trades=num_trades,
        )

        self.log.info(
            f"Successfully backtested SMA Cross for {self.ticker}:\n {json.dumps(res.dict(), indent=2)}\n"
        )

        return res, trade_book
