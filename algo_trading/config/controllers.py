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
            cls.ma_200.value: "REAL",
            cls.ma_50.value: "REAL",
            cls.ma_21.value: "REAL",
            cls.ma_7.value: "REAL",
        }

    @classmethod
    def sma_calculations(cls) -> Dict:
        return {
            cls.ma_200.value: 200,
            cls.ma_50.value: 50,
            cls.ma_21.value: 21,
            cls.ma_7.value: 7,
        }


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


class SMACrossInfo(BaseModel):

    last_cross_up: str = "1900-01-01"
    last_cross_down: str = "1900-01-02"
    last_status: StockStatusController = StockStatusController.sell
