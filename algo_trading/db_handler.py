import datetime
from typing import Dict, List, Union

import pandas as pd
import sqlalchemy as sa
import yaml as yl

import schemas


class PostgresHandler:
    def __init__(self, ticker_list: List, db_info: Dict) -> None:
        self.ticker_list = ticker_list
        self.db_info = db_info

        self.db_engine = None

        self._initialize_db_connection()

    def create_new_ticker_tables(self) -> List:
        new_tickers = self._get_new_tickers()
        if len(new_tickers) > 0:
            print(f"Found {len(new_tickers)} new tickers.")
        for ticker in new_tickers:
            self._create_table(ticker)
        return new_tickers

    def add_hist_data(self, ticker: str, df: pd.DataFrame) -> None:
        print(f"Adding {len(df)} rows to {ticker}.")
        df.to_sql(ticker, con=self.db_engine, if_exists="append", index=False)

    # TODO: Function will look at database and see what last entry date is
    def get_most_recent_date(db_conn, ticker: str) -> str:
        pass

    def _initialize_db_connection(self) -> None:
        user = self.db_info.get("user")
        password = self.db_info.get("password")
        database = self.db_info.get("database")
        host = self.db_info.get("host")
        port = self.db_info.get("port", "5432")

        self.db_engine = sa.create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        )

    def _create_table(self, ticker: str) -> None:
        col_str = ", ".join([f"{k} {v}" for k, v in schemas.DFColumns.columns.items()])
        self.db_engine.execute(f"""CREATE TABLE IF NOT EXISTS {ticker} ({col_str})""")

    def _get_new_tickers(self) -> List:
        with self.db_engine.connect() as conn:
            res = conn.execute(
                """SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"""
            )
            current_tickers = [item[0] for item in res]
        return list(set(self.ticker_list) - set(current_tickers))


def get_db_handler(handler: str) -> Union[PostgresHandler]:
    if handler == "postgres":
        return PostgresHandler
