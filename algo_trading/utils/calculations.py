from algo_trading.config.controllers import ColumnController


class Calculator:
    @staticmethod
    def calculate_sma(df, rolling_col):
        for col, val in ColumnController.sma_calculations().items():
            df[col] = df[rolling_col].rolling(val).mean()
        return df
    
    @staticmethod
    def calculate_ema(df,rolling_col):
        for col, val in ColumnController.macd_calculations().items():
            df[col] = df[rolling_col].ewm(span=val,adjust=False, min_periods=val).mean()
    
    @staticmethod
    def calculate_macd(df,rolling_col):
        for col,val in ColumnController.macd_calculations().items():
            df[col] = 
