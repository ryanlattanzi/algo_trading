from typing import Dict, List, Union

import pandas as pd
import sqlalchemy as sa

from schemas import DBColumns


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

    def df_to_sql(self, ticker: str, df: pd.DataFrame) -> None:
        print(f"Adding {len(df)} rows to {ticker}.")
        df.to_sql(ticker, con=self.db_engine, if_exists="append", index=False)

    def get_most_recent_date(self, ticker: str) -> str:
        query_line = f"""SELECT DATE FROM {ticker} ORDER BY DATE DESC LIMIT 1"""
        with self.db_engine.connect() as connection:
            last_entry_date = connection.execute(query_line)
        # need to find a better way to parse query results
        return [item[0] for item in last_entry_date][0]

    def get_data(self, ticker: str, condition: str = "") -> pd.DataFrame:
        query = f"SELECT * FROM {ticker} {condition}".strip()
        return pd.read_sql(query, con=self.db_engine)

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
        print(f"Creating table {ticker}")
        col_str = ", ".join([f"{k} {v}" for k, v in DBColumns.columns().items()])
        self.db_engine.execute(f"""CREATE TABLE IF NOT EXISTS {ticker} ({col_str})""")

    def _get_new_tickers(self) -> List:
        with self.db_engine.connect() as conn:
            res = conn.execute(
                """SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'public'"""
            )
            current_tickers = [item[0] for item in res]
        return list(set(self.ticker_list) - set(current_tickers))


def get_db_handler(
    handler: str, ticker_list: List, db_info: Dict
) -> Union[PostgresHandler]:
    if handler == "postgres":
        return PostgresHandler(ticker_list, db_info)
