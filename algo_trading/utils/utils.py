from typing import List
from datetime import datetime
import pandas as pd
from sqlalchemy.engine.base import Engine

from algo_trading.config.controllers import ColumnController

DATE_FORMAT = "%Y-%m-%d"


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_headers(df)
    df = validate_columns(df)
    df = dedupe_by_date(df)
    return df


def clean_headers(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = snake_case(list(df.columns))
    return df


def validate_columns(df: pd.DataFrame) -> pd.DataFrame:
    if not list(df.columns) == ColumnController.df_columns():
        raise ValueError("DF Columns do not adhere to the schema.")
    return df


def dedupe_by_date(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates(subset=[ColumnController.date.value])


def snake_case(cols: List) -> List:
    return [x.lower().replace(" ", "_") for x in cols]


def str_to_dt(date_str: str, fmt: str = DATE_FORMAT) -> datetime:
    return datetime.strptime(date_str, fmt)


def dt_to_str(dt: datetime, fmt: str = DATE_FORMAT) -> str:
    return dt.strftime(fmt)


def convert_date_col_to_datetime(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    df[date_col] = pd.to_datetime(df[date_col])
    return df


def read_sql_to_df(query: str, con: Engine) -> pd.DataFrame:
    df = pd.read_sql(query, con=con)
    if ColumnController.date.value in df.columns:
        df = convert_date_col_to_datetime(df, ColumnController.date.value)
    return df
