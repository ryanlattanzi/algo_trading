from typing import Dict, Optional, Union
import pandas as pd
from pydantic import validate_arguments

from algo_trading.strategies.sma_cross_strat import SMACross
from algo_trading.repositories.db_repository import AbstractDBRepository, DBRepository
from algo_trading.repositories.key_val_repository import KeyValueRepository
from algo_trading.config.controllers import (
    ColumnController,
    DBHandlerController,
    KeyValueController,
    StockStatusController,
)
from algo_trading.utils.utils import str_to_dt
from algo_trading.strategies.events import TradeEvent

from dags.sma_cross_dag import update_redis

from controllers import TestPeriodController


class SMACrossBackTester:

    _days_back = {
        TestPeriodController.one_mo: 30,
        TestPeriodController.three_mo: 90,
        TestPeriodController.six_mo: 180,
        TestPeriodController.one_yr: 365,
        TestPeriodController.two_yr: 730,
        TestPeriodController.five_yr: 1825,
        TestPeriodController.ten_yr: 3650,
        TestPeriodController.max: "max",
    }

    @validate_arguments(config=dict(arbitrary_types_allowed=True))
    def __init__(
        self,
        ticker: str,
        db_info: Dict,
        db_handler: DBHandlerController,
        period: Optional[TestPeriodController] = None,
        start_date: Optional[str] = None,
        capital: int = 10000,
    ) -> None:
        """Initializer for the SMA Back tester. Provide EITHER
        a period OR a start_date, but not both.

        Args:
            ticker (str): Ticker to test
            db_info (Dict): Live DB to query to get price history data
            db_handler (DBHandlerController): Type of live DB.
            period (Optional[TestPeriodController], optional): Period to test. Defaults to None.
            start_date (Optional[str], optional): Start date to test. Defaults to None.
            capital (int, optional): Starting cash. Defaults to 10000.

        Raises:
            ValueError: Error if both period and start_date are supplied.
        """
        self.ticker = ticker
        self.db_info = db_info
        self.db_handler = db_handler
        self.period = period
        self.start_date = start_date
        self.capital = capital

        if self.period and self.start_date:
            raise ValueError("Please include either a period or a start date, not both")

        self.fake_db_repo = None
        self.fake_kv_repo = None

    @property
    def days_back(self) -> Union[int, str, None]:
        """Returns the number of days to query back if a period
        is supplied to the class.

        Returns:
            Union[int, str, None]: Returns None, integer, or 'max'
        """
        if self.period:
            return SMACrossBackTester._days_back[self.period]
        else:
            return None

    @property
    def db_repo(self) -> AbstractDBRepository:
        """Live prices DB to pull data from to test.

        Returns:
            AbstractDBRepository: DB Repo handler object.
        """
        try:
            return self._db_repo
        except AttributeError:
            self._db_repo = DBRepository(self.db_info, self.db_handler).handler
            return self._db_repo

    @property
    def price_data(self) -> pd.DataFrame:
        """Pulling data from the real DB. This is why we needed db_info.
        Caches this data for testing so we don't have to clog the
        live DB.

        Returns:
            pd.DataFrame: Price DF to use for testing.
        """
        try:
            return self._price_data
        except AttributeError:
            if self.period:
                if self.period == TestPeriodController.max:
                    data = self.db_repo.get_all(self.ticker)
                else:
                    data = self.db_repo.get_days_back(self.ticker, self.days_back)
            elif self.start_date:
                data = self.db_repo.get_since_date(self.ticker, self.start_date)
            self._price_data = data.sort_values(
                [ColumnController.date.value], ascending=True
            ).iloc[1:]
            return self._price_data

    def _get_num_shares(self, cash: float, share_price: float) -> float:
        """Simple calculation of num shares to buy.

        Args:
            cash (float): Current cash
            share_price (float): Share price

        Returns:
            float: Num shares to purchase
        """
        return cash / share_price

    def _get_new_capital(self, num_shares: float, share_price: float) -> float:
        """Simple calculation of capitcal after selling.

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
        """Generate percent change from starting and ending capital.

        Args:
            start_cap (float): Starting capital
            end_cap (float): Ending capital
            precision (int, optional): Decimal places. Defaults to 2.

        Returns:
            float: Percent change
        """
        return round(((end_cap - start_cap) / start_cap) * 100, precision)

    def test(self) -> None:
        """Runs the SMA strategy over the cached price data.
        Instantiates a fake DBRepository and a fake KeyValueRepository
        to 'query' from.

        Calculates percent gain/loss after
        """
        fake_db_repo = DBRepository(self.price_data, DBHandlerController.fake)
        fake_kv_repo = KeyValueRepository(
            kv_info={
                ColumnController.last_cross_up.value: None,
                ColumnController.last_cross_down.value: None,
                ColumnController.last_status.value: StockStatusController.wait.value,
            },
            kv_handler=KeyValueController.fake,
        )

        sma = SMACross(self.ticker, fake_db_repo, fake_kv_repo)
        starting_cap = self.capital
        num_shares = 0
        state = "sell"
        for idx, row in self.price_data.iterrows():
            result: TradeEvent = sma.check_sma_cross()
            if result.signal == StockStatusController.buy:
                if state == "sell":
                    num_shares = self._get_num_shares(
                        self.capital, row[ColumnController.close.value]
                    )
                    print(
                        f"Bought {num_shares} shares at price {row[ColumnController.close.value]} "
                        + f"on {row[ColumnController.date.value]}."
                    )
                    state = "buy"
            elif result.signal == StockStatusController.sell:
                if state == "buy":
                    self.capital = self._get_new_capital(
                        num_shares, row[ColumnController.close.value]
                    )
                    print(
                        f"Sold {num_shares} shares at price "
                        + f"{row[ColumnController.close.value]} on "
                        + f"{row[ColumnController.date.value]} for a new capital of {self.capital}."
                    )
                    num_shares = 0
                    state = "sell"
        if num_shares != 0:
            self.capital = self._get_new_capital(
                num_shares, self.price_data.iloc[-1][ColumnController.close.value]
            )
            print(
                f"Sold {num_shares} shares at price "
                + f"{self.price_data.iloc[-1][ColumnController.close.value]} "
                + f"on {self.price_data.iloc[-1][ColumnController.date.value]} "
                + f"for a new capital of {self.capital}."
            )
        percent_change = self._get_percent_change(starting_cap, self.capital)
        print(f"Starting cap: {starting_cap}, final cap: {self.capital}.")
        print(f"Change over {self.period.value} is {percent_change} %.")


if __name__ == "__main__":
    from algo_trading.constants import DB_INFO

    ticker = "aapl"
    tester = SMACrossBackTester(ticker, DB_INFO, "postgres", "max", capital=1000)
    tester.test()