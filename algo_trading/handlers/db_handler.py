from typing import Dict, List
from abc import ABC, abstractproperty


import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine.base import Engine

from algo_trading.config.controllers import ColumnController, DBTypeController
from algo_trading.utils.utils import dt_to_str


class AbstractDBRepository(ABC):
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
    def create_table(self) -> str:
        pass

    @abstractproperty
    def get_current_tickers(self) -> str:
        pass


class PostgresRepository(AbstractDBRepository):
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
    def create_table(self) -> str:
        return "CREATE TABLE IF NOT EXISTS %s (%s)"

    @property
    def get_current_tickers(self) -> str:
        return "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'public'"


class DBRepository:

    db_types = {
        "postgres": PostgresRepository,
    }

    def __init__(
        self,
        ticker_list: List,
        db_info: Dict,
        db_type: DBTypeController,
    ) -> None:

        self.ticker_list = ticker_list
        self.db_info = db_info
        self.db_type = db_type

    @property
    def db_repo(self) -> AbstractDBRepository:
        return DBRepository.db_types[self.db_type](**self.db_info)

    @property
    def db_engine(self) -> Engine:
        try:
            return self._db_engine
        except AttributeError:
            self._db_engine = sa.create_engine(self.db_repo.sql_alchemy_conn_str)
            return self._db_engine

    def _create_table(self, ticker: str) -> None:
        print(f"Creating table {ticker}")
        col_str = ", ".join(
            [f"{k} {v}" for k, v in ColumnController.db_columns().items()]
        )
        query = self.db_repo.create_table % (ticker, col_str)
        with self.db_engine.connect() as conn:
            conn.execute(query)

    def _get_new_tickers(self) -> List[str]:
        with self.db_engine.connect() as conn:
            res = conn.execute(self.db_repo.get_current_tickers)
            current_tickers = [item[0] for item in res.fetchall()]
        return list(set(self.ticker_list) - set(current_tickers))

    def create_new_ticker_tables(self) -> List:
        new_tickers = self._get_new_tickers()
        print(f"Processing {len(new_tickers)} new ticker(s).")
        for ticker in new_tickers:
            self._create_table(ticker)
        return new_tickers

    def append_df_to_sql(self, ticker: str, df: pd.DataFrame) -> None:
        print(f"Adding {len(df)} rows to {ticker}.")
        df.to_sql(ticker, con=self.db_engine, if_exists="append", index=False)

    def get_most_recent_date(self, ticker: str) -> str:
        query = self.db_repo.get_most_recent_date % (ticker,)
        with self.db_engine.connect() as conn:
            res = conn.execute(query)
        return dt_to_str(res.fetchall()[0][0])

    def get_days_back(self, ticker: str, days_back: int) -> pd.DataFrame:
        query = self.db_repo.get_days_back % (ticker, days_back)
        return pd.read_sql(query, con=self.db_engine)
