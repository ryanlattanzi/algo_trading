from enum import Enum
from typing import List, Dict


class ColumnController(Enum):
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
