from enum import Enum
from pydantic import BaseModel
from typing import List, Dict


class ColumnController(Enum):
    # DB stuff
    date = "date"
    open = "open"
    high = "high"
    low = "low"
    close = "close"
    adj_close = "adj_close"
    volume = "volume"
    ma_200 = "ma_200"
    ma_50 = "ma_50"
    ma_21 = "ma_21"
    ma_7 = "ma_7"
    ema_12 = "ema_12"
    ema_26 = "ema_26"
    macd_line = "macd_line"
    signal_line = "signal_line"

    @classmethod
    def df_columns(cls) -> List[str]:
        return [
            cls.date.value,
            cls.open.value,
            cls.high.value,
            cls.low.value,
            cls.close.value,
            cls.adj_close.value,
            cls.volume.value,
        ]

    @classmethod
    def db_columns(cls) -> Dict:
        return {
            cls.date.value: "DATE",
            cls.open.value: "REAL",
            cls.high.value: "REAL",
            cls.low.value: "REAL",
            cls.close.value: "REAL",
            cls.adj_close.value: "REAL",
            cls.volume.value: "BIGINT",
        }

    @classmethod
    def sma_calculations(cls) -> Dict:
        return {
            cls.ma_200.value: 200,
            cls.ma_50.value: 50,
            cls.ma_21.value: 21,
            cls.ma_7.value: 7,
        }

    @classmethod
    def ema_calculations(cls) -> Dict:
        return {
            cls.ema_26.value: 26,
            cls.ema_12.value: 12,
        }

    @classmethod
    def macd_calculations(cls) -> Dict:
        return {cls.signal_line.value: 9}


class StockStatusController(str, Enum):
    buy = "BUY"
    sell = "SELL"
    wait = "WAIT"
    hold = "HOLD"


class DBHandlerController(str, Enum):
    fake = "fake"
    postgres = "postgres"


class DataHandlerController(str, Enum):
    yahoo_finance = "yahoo_finance"


class KeyValueController(str, Enum):
    fake = "fake"
    redis = "redis"


class ObjStoreController(str, Enum):
    minio = "minio"
    s3 = "s3"


class PubSubController(str, Enum):
    redis = "redis"


class TestPeriodController(str, Enum):
    one_mo = "1mo"
    three_mo = "3mo"
    six_mo = "6mo"
    one_yr = "1yr"
    two_yr = "2yr"
    five_yr = "5yr"
    ten_yr = "10yr"
    max = "max"


class Config(BaseModel):

    ticker_list: List
    db_repo: str
    data_repo: str
    kv_repo: str
    obj_store_repo: str


class StrategyInfo(BaseModel):

    sma_last_cross_up: str = "1900-01-01"
    sma_last_cross_down: str = "1900-01-02"
    sma_last_status: StockStatusController = StockStatusController.sell

    macd_last_cross_up: str = "1900-01-01"
    macd_last_cross_down: str = "1900-01-02"
    macd_last_status: StockStatusController = StockStatusController.sell
