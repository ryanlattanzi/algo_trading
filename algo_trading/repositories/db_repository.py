from typing import Dict, List, Optional, Union
from abc import ABC, abstractmethod, abstractproperty
from pydantic import validate_arguments


import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine.base import Engine

from algo_trading.config.controllers import ColumnController, DBHandlerController
from algo_trading.utils.utils import dt_to_str


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
        return "SELECT DATE FROM %s ORDER BY DATE DESC LIMIT 1"

    @property
    def get_days_back(self) -> str:
        return "SELECT * FROM %s ORDER BY DATE DESC LIMIT %s"

    @property
    def get_since_date(self) -> str:
        return "SELECT * FROM %s WHERE DATE >= '%s'"

    @property
    def get_all(self) -> str:
        return "SELECT * FROM %s ORDER BY DATE DESC"

    @property
    def create_table(self) -> str:
        return "CREATE TABLE IF NOT EXISTS %s (%s)"

    @property
    def get_current_tickers(self) -> str:
        return "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'public'"


class AbstractDBRepository(ABC):
    """Abstract repository to define common methods that
    interact with a DB.

    Args:
        ABC ([type]): Abstract Base Class
    """

    @abstractmethod
    def get_days_back(self, ticker: str, days_back: int) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_since_date(self, ticker: str, date: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_all(self, ticker: str) -> pd.DataFrame:
        pass


class FakeDBRepository(AbstractDBRepository):
    def __init__(self, data: pd.DataFrame) -> None:
        """Fake DB repo that accepts data as a DF to act as
        the table in the live price DB.

        We add the idx_iterator because we assume that you
        are using this for a test and want to iterate through
        the DF to test strategies.

        Args:
            data (pd.DataFrame): Data to 'query' from.
        """
        self.data = data

        self.idx_iterator = 0

    def get_days_back(self, ticker: str, days_back: int) -> pd.DataFrame:
        if (self.idx_iterator - days_back) < 0:
            start_idx = 0
        else:
            start_idx = self.idx_iterator - days_back
        data = self.data.iloc[start_idx : (self.idx_iterator)]
        self.idx_iterator += 1
        return data

    def get_since_date(self, ticker: str, date: str) -> pd.DataFrame:
        pass

    def get_all(self, ticker: str) -> pd.DataFrame:
        return self.data


class PostgresRepository(AbstractDBRepository):
    def __init__(self, db_info: Dict) -> None:
        """DB Repository that taps into a Postgres instance.
        There are a few added methods to the raw AbstractDBRepository
        to handle some business scenarios.

        Args:
            db_info (Dict): DB connection information.
        """
        self.db_info = db_info

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
        print(f"Creating table {ticker}")
        col_str = ", ".join(
            [f"{k} {v}" for k, v in ColumnController.db_columns().items()]
        )
        query = self.queries.create_table % (ticker, col_str)
        with self.db_engine.connect() as conn:
            conn.execute(query)

    def _get_new_tickers(self, tickers: List[str]) -> List[str]:
        with self.db_engine.connect() as conn:
            res = conn.execute(self.queries.get_current_tickers)
            current_tickers = [item[0] for item in res.fetchall()]
        return list(set(tickers) - set(current_tickers))

    def create_new_ticker_tables(self, tickers: List[str]) -> List:
        new_tickers = self._get_new_tickers(tickers)
        print(f"Processing {len(new_tickers)} new ticker(s).")
        for ticker in new_tickers:
            self._create_table(ticker)
        return new_tickers

    def append_df_to_sql(self, ticker: str, df: pd.DataFrame) -> None:
        print(f"Adding {len(df)} rows to {ticker}.")
        df.to_sql(ticker, con=self.db_engine, if_exists="append", index=False)

    def get_most_recent_date(self, ticker: str) -> str:
        query = self.queries.get_most_recent_date % (ticker,)
        with self.db_engine.connect() as conn:
            res = conn.execute(query)
        return dt_to_str(res.fetchall()[0][0])

    def get_days_back(self, ticker: str, days_back: int) -> pd.DataFrame:
        query = self.queries.get_days_back % (ticker, days_back)
        return pd.read_sql(query, con=self.db_engine)

    def get_since_date(self, ticker: str, date: str) -> pd.DataFrame:
        query = self.queries.get_since_date % (ticker, date)
        return pd.read_sql(query, con=self.db_engine)

    def get_all(self, ticker: str) -> pd.DataFrame:
        query = self.queries.get_all % (ticker,)
        return pd.read_sql(query, con=self.db_engine)


class DBRepository:
    _db_handlers = {
        DBHandlerController.fake: FakeDBRepository,
        DBHandlerController.postgres: PostgresRepository,
    }

    @validate_arguments(config=dict(arbitrary_types_allowed=True))
    def __init__(
        self, db_info: Union[Dict, pd.DataFrame], db_handler: DBHandlerController
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

    @property
    def handler(self) -> AbstractDBRepository:
        return DBRepository._db_handlers[self.db_handler](self.db_info)
