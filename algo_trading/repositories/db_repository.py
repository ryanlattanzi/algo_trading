from typing import Dict, List, Union
from abc import ABC, abstractmethod, abstractproperty
from logging import Logger
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine.base import Engine
from pydantic import validate_arguments

from algo_trading.logger.controllers import LogConfig
from algo_trading.logger.default_logger import get_child_logger
from algo_trading.config.controllers import ColumnController, DBHandlerController
from algo_trading.utils.utils import dt_to_str, str_to_dt, read_sql_to_df


class AbstractQuery(ABC):
    @abstractproperty
    def sql_alchemy_conn_str(self) -> str:
        pass

    @abstractproperty
    def get_most_recent_date(self) -> str:
        pass

    @abstractproperty
    def get_days_back(self) -> str:
        pass

    @abstractproperty
    def get_since_date(self) -> str:
        pass

    @abstractproperty
    def get_all(self) -> str:
        pass

    @abstractproperty
    def create_table(self) -> str:
        pass

    @abstractproperty
    def get_current_tickers(self) -> str:
        pass


class PostgresQuery(AbstractQuery):
    def __init__(
        self,
        user: str,
        password: str,
        db_name: str,
        host: str,
        port: str,
    ) -> None:
        self.user = user
        self.password = password
        self.db_name = db_name
        self.host = host
        self.port = port

    @property
    def sql_alchemy_conn_str(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"

    @property
    def get_most_recent_date(self) -> str:
        return "SELECT {date_col} FROM {table} ORDER BY {date_col} DESC LIMIT 1;"

    @property
    def get_days_back(self) -> str:
        return """
            SELECT temp.*
            FROM (SELECT * FROM {table} ORDER BY {date_col} DESC LIMIT {days_back}) AS temp
            ORDER BY {date_col} ASC;
            """

    @property
    def get_since_date(self) -> str:
        return "SELECT * FROM {table} WHERE {date_col} >= '{since_date}' ORDER BY {date_col} ASC;"

    @property
    def get_until_date(self) -> str:
        return "SELECT * FROM {table} WHERE {date_col} <= '{until_date}' ORDER BY {date_col} ASC;"

    @property
    def get_dates_between(self) -> str:
        return "SELECT * FROM {table} WHERE {date_col} BETWEEN '{start_date}' AND '{end_date}' ORDER BY {date_col} ASC;"

    @property
    def get_all(self) -> str:
        return "SELECT * FROM {table} ORDER BY {date_col} ASC;"

    @property
    def get_row_num(self) -> str:
        return "SELECT ROW_NUMBER() OVER() AS ROW_NUM, {date_col} FROM {table} ORDER BY {date_col} ASC;"

    @property
    def create_table(self) -> str:
        return "CREATE TABLE IF NOT EXISTS {table} ({ddl_str});"

    @property
    def get_current_tickers(self) -> str:
        return "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'public';"


class AbstractDBRepository(ABC):
    """Abstract repository to define common methods that
    interact with a DB.

    Args:
        ABC ([type]): Abstract Base Class
    """

    @abstractmethod
    def create_new_ticker_tables(self, tickers: List[str]) -> List:
        """Creates tables for new tickers. Will need to implement
        logic to find out the newest tickers given the total list
        of tickers. This can be achieved by querying the DB to see
        which tables exist.

        Args:
            tickers (List[str]): Total list of tickers

        Returns:
            List: List of new tickers.
        """
        pass

    @abstractmethod
    def get_days_back(self, ticker: str, days_back: int) -> pd.DataFrame:
        """Gets days_back rows of price data in ASCENDING order by date.

        Args:
            ticker (str): Ticker to fetch data.
            days_back (int): Last n rows to fetch.

        Returns:
            pd.DataFrame: Price data in ASCENDING order by date.
        """
        pass

    @abstractmethod
    def get_since_date(self, ticker: str, date: str) -> pd.DataFrame:
        """Gets data since the given date for the ticker. Returns
        price data in ASCENDING order by date.

        Args:
            ticker (str): Ticker to fetch data.
            date (str): Lower bound date from which to grab data.

        Returns:
            pd.DataFrame: Price data in ASCENDING order by date.
        """
        pass

    @abstractmethod
    def get_until_date(self, ticker: str, date: str) -> pd.DataFrame:
        """Gets data until the given date for the ticker (inclusive).
        Returns price data in ASCENDING order by date.

        Args:
            ticker (str): Ticker to fetch data
            date (str): Upper bound date from which to grab data.

        Returns:
            pd.DataFrame: Price data in ASCENDING order by date.
        """
        pass

    @abstractmethod
    def get_dates_between(
        self, ticker: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """Gets data between the two given dates (inclusive). Returns
        data in ASCENDING order by date.

        Args:
            ticker (str): Ticker to fetch data.
            start_date (str): Start date of price data.
            end_date (str): End date of price data.

        Returns:
            pd.DataFrame: Price data in ASCENDING order by date.
        """
        pass

    @abstractmethod
    def get_row_num(self, ticker: str) -> pd.DataFrame:
        """Gets the row number for each date in the table. Returns
        a dataframe with ROW_NUM and DATE columns.

        Args:
            ticker (str): Ticker to fetch data.

        Returns:
            pd.DataFrame: Dataframe with all dates and corresponding
                          row numbers.
        """
        pass

    @abstractmethod
    def get_all(self, ticker: str) -> pd.DataFrame:
        """Gets all data for the ticker. Returns price data in
        ASCENDING order by date.

        Args:
            ticker (str): Ticker to fetch data.

        Returns:
            pd.DataFrame: Price data in ASCENDING order by date.
        """
        pass


class FakeDBRepository(AbstractDBRepository):
    def __init__(
        self,
        db_info: Dict,
        log_info: LogConfig,
    ) -> None:
        """Fake DB repo that accepts data as a DF to act as
        the table in the live price DB. The DF is in ASCENDING
        order by date.

        We add the idx_iterator because we assume that you
        are using this for a test and want to iterate through
        the DF to test strategies.

        Args:
            db_info (Dict): Init dict containing data and starting index.
            log_info (LogConfig): log info
        """
        self.db_info = db_info
        self.log_info = log_info

        self.data: pd.DataFrame = self.db_info["data"]
        self.idx_iterator = self.db_info.get("idx_iterator", 0)

    def _get_idx_from_date(self, date: str, default="max") -> int:
        try:
            return self.data.index[
                self.data[ColumnController.date.value] == str_to_dt(date)
            ].tolist()[0]
        except IndexError:
            if default == "max":
                return len(self.data) - 1
            else:
                return int(0)

    def create_new_ticker_tables(self, tickers: List[str]) -> List:
        pass

    def get_days_back(self, ticker: str, days_back: int) -> pd.DataFrame:
        if (self.idx_iterator - days_back) < 0:
            start_idx = 0
        else:
            start_idx = self.idx_iterator - days_back

        data = self.data.iloc[start_idx : (self.idx_iterator + 1)]
        self.idx_iterator += 1
        return data

    def get_since_date(self, ticker: str, date: str) -> pd.DataFrame:
        since_date_idx = self._get_idx_from_date(date, default="min")
        return self.data.iloc[since_date_idx:].reset_index(drop=True)

    def get_until_date(self, ticker: str, date: str) -> pd.DataFrame:
        until_date_idx = self._get_idx_from_date(date)
        return self.data.iloc[: (until_date_idx + 1)].reset_index(drop=True)

    def get_dates_between(
        self, ticker: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        start_date_idx = self._get_idx_from_date(start_date, default="min")
        end_date_idx = self._get_idx_from_date(end_date)
        return self.data.iloc[start_date_idx : (end_date_idx + 1)].reset_index(
            drop=True
        )

    def get_row_num(self, ticker: str) -> pd.DataFrame:
        idx_df = self.data.reset_index()
        return idx_df[["index", ColumnController.date.value]]

    def get_all(self, ticker: str) -> pd.DataFrame:
        return self.data


class PostgresRepository(AbstractDBRepository):

    date_col = ColumnController.date.value

    def __init__(self, db_info: Dict, log_info: LogConfig) -> None:
        """DB Repository that taps into a Postgres instance.
        There are a few added methods to the raw AbstractDBRepository
        to handle some business scenarios.

        Args:
            db_info (Dict): DB connection information.
        """
        self.db_info = db_info
        self.log_info = log_info

    @property
    def log(self) -> Logger:
        try:
            return self._log
        except AttributeError:
            self._log = get_child_logger(
                self.log_info.log_name, self.__class__.__name__
            )
            return self._log

    @property
    def queries(self) -> AbstractQuery:
        try:
            return self._queries
        except AttributeError:
            self._queries = PostgresQuery(**self.db_info)
        return self._queries

    @property
    def db_engine(self) -> Engine:
        try:
            return self._db_engine
        except AttributeError:
            self._db_engine = sa.create_engine(self.queries.sql_alchemy_conn_str)
            return self._db_engine

    def _create_table(self, ticker: str) -> None:
        self.log.info(f"Creating table {ticker}")
        col_str = ", ".join(
            [f"{k} {v}" for k, v in ColumnController.db_columns().items()]
        )
        query = self.queries.create_table.format(table=ticker, ddl_str=col_str)
        with self.db_engine.connect() as conn:
            conn.execute(query)

    def _get_new_tickers(self, tickers: List[str]) -> List[str]:
        with self.db_engine.connect() as conn:
            res = conn.execute(self.queries.get_current_tickers)
            current_tickers = [item[0] for item in res.fetchall()]
        return list(set(tickers) - set(current_tickers))

    def create_new_ticker_tables(self, tickers: List[str]) -> List:
        new_tickers = self._get_new_tickers(tickers)
        self.log.info(f"Processing {len(new_tickers)} new ticker(s).")
        for ticker in new_tickers:
            self._create_table(ticker)
        return new_tickers

    def append_df_to_sql(self, ticker: str, df: pd.DataFrame) -> None:
        self.log.info(f"Adding {len(df)} rows to {ticker}.")
        df.to_sql(ticker, con=self.db_engine, if_exists="append", index=False)

    def get_most_recent_date(self, ticker: str) -> str:
        query = self.queries.get_most_recent_date.format(
            date_col=self.date_col, table=ticker
        )
        with self.db_engine.connect() as conn:
            res = conn.execute(query)
        return dt_to_str(res.fetchall()[0][0])

    def get_days_back(self, ticker: str, days_back: int) -> pd.DataFrame:
        query = self.queries.get_days_back.format(
            table=ticker, date_col=self.date_col, days_back=days_back
        )
        return read_sql_to_df(query, self.db_engine)

    def get_since_date(self, ticker: str, date: str) -> pd.DataFrame:
        query = self.queries.get_since_date.format(
            table=ticker, date_col=self.date_col, since_date=date
        )
        return read_sql_to_df(query, self.db_engine)

    def get_until_date(self, ticker: str, date: str) -> pd.DataFrame:
        query = self.queries.get_until_date.format(
            table=ticker, date_col=self.date_col, until_date=date
        )
        return read_sql_to_df(query, self.db_engine)

    def get_dates_between(
        self, ticker: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        query = self.queries.get_dates_between.format(
            table=ticker,
            date_col=self.date_col,
            start_date=start_date,
            end_date=end_date,
        )
        return read_sql_to_df(query, self.db_engine)

    def get_row_num(self, ticker: str) -> pd.DataFrame:
        query = self.queries.get_row_num.format(table=ticker, date_col=self.date_col)
        return read_sql_to_df(query, self.db_engine)

    def get_all(self, ticker: str) -> pd.DataFrame:
        query = self.queries.get_all.format(table=ticker, date_col=self.date_col)
        return read_sql_to_df(query, self.db_engine)


class DBRepository:
    _db_handlers = {
        DBHandlerController.fake: FakeDBRepository,
        DBHandlerController.postgres: PostgresRepository,
    }

    # For some reason, validate_arguments is casting the pandas DF used
    # as an arg in the FakeDBRepository to a dict, so we leave it out here.

    # @validate_arguments(config=dict(arbitrary_types_allowed=True))
    def __init__(
        self,
        db_info: Dict,
        db_handler: DBHandlerController,
        log_info: LogConfig,
    ) -> None:
        """A wrapper class to provide a consistent interface to the
        different DBRepository types found in the _db_handlers class
        attribute.

        Args:
            db_info (Union[Dict, pd.DataFrame]): Info to instantiate the DB object.
            db_handler (DBHandlerController): Type of DB repo to fetch.
        """
        self.db_info = db_info
        self.db_handler = db_handler
        self.log_info = log_info

    @property
    def handler(self) -> AbstractDBRepository:
        return DBRepository._db_handlers[self.db_handler](self.db_info, self.log_info)
