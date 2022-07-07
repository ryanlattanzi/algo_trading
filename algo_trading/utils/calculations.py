import pandas as pd
from typing import Dict
from algo_trading.config.controllers import ColumnController


class Calculator:
    @staticmethod
    def calculate_sma(df: pd.DataFrame, rolling_col: str) -> pd.DataFrame:
        """_summary_

        Args:
            df (pd.DataFrame): _description_
            rolling_col (str): _description_

        Returns:
            pd.DataFrame: _description_
        """
        for col, val in ColumnController.sma_calculations().items():
            df[col] = df[rolling_col].rolling(val).mean()
        return df

    @staticmethod
    def calculate_ema(
        df: pd.DataFrame, rolling_col: str, ema_col_dict: Dict
    ) -> pd.DataFrame:
        """_summary_

        Args:
            df (pd.DataFrame): _description_
            rolling_col (str): _description_
            ema_col_dict (Dict): _description_

        Returns:
            pd.DataFrame: _description_
        """
        for col, val in ema_col_dict.items():
            df[col] = (
                df[rolling_col].ewm(span=val, adjust=False, min_periods=val).mean()
            )
        return df

    @staticmethod
    def calculate_macd_signal(
        df: pd.DataFrame, macd_fast: str, macd_slow: str
    ) -> pd.DataFrame:
        """_summary_

        Args:
            df (pd.DataFrame): _description_
            macd_fast (str): _description_
            macd_slow (str): _description_

        Returns:
            pd.DataFrame: _description_
        """
        df[ColumnController.macd_line.value] = df[macd_fast] - df[macd_slow]

        df = Calculator.calculate_ema(
            df,
            ColumnController.macd_line.value,
            ColumnController.macd_calculations(),
        )

        return df
