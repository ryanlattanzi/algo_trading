from typing import List

import pandas as pd

import schemas


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_headers(df)
    df = validate_columns(df)
    return df


# TODO: Function will look at database and see what last entry date is
def get_most_recent_date(db_conn, ticker: str) -> str:
    pass


def clean_headers(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = snake_case(list(df.columns))
    return df


def validate_columns(df: pd.DataFrame) -> pd.DataFrame:
    if not list(df.columns) == list(schemas.DFColumns.columns.keys()):
        raise ValueError
    return df


def snake_case(cols: List) -> List:
    return [x.lower().replace(" ", "_") for x in cols]
