import pandas as pd

from algo_trading.repositories.db_repository import FakeDBRepository

data = pd.read_csv("sample_data.csv")


def test_step_through():
    fake_db = FakeDBRepository(data)
    print(fake_db.idx_iterator)
    for idx, row in fake_db.data[:100].iterrows():
        one_day_back = fake_db.get_days_back("aapl", 1)
        print(one_day_back.to_dict("records"))


if __name__ == "__main__":
    test_step_through()
