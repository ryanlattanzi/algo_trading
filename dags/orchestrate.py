from typing import Dict, List, Tuple

from algo_trading.config import CONFIG, DATA_BUCKET, LOG_BUCKET

import data_pull_dag
import sma_cross_dag


def orchestrate_data_pull_dag() -> Tuple[List[str], Dict[str, Dict[str, str]]]:
    data_pull_dag.create_bucket(DATA_BUCKET)
    data_pull_dag.create_bucket(LOG_BUCKET)

    new_tickers = data_pull_dag.create_new_tables(CONFIG.ticker_list)
    new_ticker_data = data_pull_dag.get_new_ticker_data(
        CONFIG.data_repo,
        new_tickers,
    )
    new_ticker_paths = data_pull_dag.persist_ticker_data(new_ticker_data)
    existing_ticker_data = data_pull_dag.get_existing_ticker_data(
        CONFIG.data_repo,
        CONFIG.ticker_list,
        new_tickers,
    )
    _ = data_pull_dag.persist_ticker_data(existing_ticker_data)
    data_pull_dag.finish_log()
    data_pull_dag.persist_log()
    return new_tickers, new_ticker_paths


def orchestrate_sma_cross_dag(new_tickers: List, new_ticker_paths: Dict) -> None:
    sma_cross_dag.backfill_redis(new_ticker_paths)
    sma_cross_dag.update_redis(CONFIG.ticker_list, new_tickers)


if __name__ == "__main__":
    new_tickers, new_ticker_paths = orchestrate_data_pull_dag()
    orchestrate_sma_cross_dag(new_tickers, new_ticker_paths)
