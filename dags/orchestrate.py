from typing import List

from algo_trading.config.events import TradeEvent

from config import CONFIG, LOG_BUCKET
import data_pull_dag
import sma_cross_dag
import macd_cross_dag


def orchestrate_data_pull_dag() -> List[str]:
    data_pull_dag.create_bucket(LOG_BUCKET)

    new_tickers = data_pull_dag.create_new_tables(CONFIG.ticker_list)
    new_ticker_data = data_pull_dag.get_new_ticker_data(
        CONFIG.data_repo,
        new_tickers,
    )
    data_pull_dag.persist_ticker_data(new_ticker_data)
    existing_ticker_data = data_pull_dag.get_existing_ticker_data(
        CONFIG.data_repo,
        CONFIG.ticker_list,
        new_tickers,
    )
    data_pull_dag.persist_ticker_data(existing_ticker_data)
    data_pull_dag.finish_log()
    data_pull_dag.persist_log()
    return new_tickers


def orchestrate_sma_cross_dag(new_tickers: List) -> List[TradeEvent]:
    sma_cross_dag.backfill_redis(new_tickers)
    sma_cross_dag.update_redis(CONFIG.ticker_list, new_tickers)
    events = sma_cross_dag.run_sma(CONFIG.ticker_list)
    sma_cross_dag.finish_log()
    sma_cross_dag.persist_log()
    return events


def orchestrate_macd_cross_dag(new_tickers: List) -> List[TradeEvent]:
    macd_cross_dag.backfill_redis(new_tickers)
    macd_cross_dag.update_redis(CONFIG.ticker_list, new_tickers)
    events = macd_cross_dag.run_macd(CONFIG.ticker_list)
    macd_cross_dag.finish_log()
    macd_cross_dag.persist_log()
    return events


if __name__ == "__main__":
    new_tickers = orchestrate_data_pull_dag()
    sma_events = orchestrate_sma_cross_dag(new_tickers)
    macd_events = orchestrate_macd_cross_dag(new_tickers)
