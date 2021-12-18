from pydantic import BaseModel


class BackTestResult(BaseModel):

    ticker: str
    start_date: str
    end_date: str
    init_cap: float
    final_cap: float
    cap_gains: float
    num_trades: int
