#!/bin/bash

echo "Backtesting AAPL from max date"
curl -X 'POST' \
  'http://localhost:8005/backtest' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "ticker": "AAPL",
  "strategy": "sma_cross",
  "start_date": "max",
  "end_date": "2022-03-06",
  "starting_capital": 1000
}'
echo

echo "Backtesting AAPL from more recent date"
curl -X 'POST' \
  'http://localhost:8005/backtest' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "ticker": "AAPL",
  "strategy": "sma_cross",
  "start_date": "2000-01-01",
  "end_date": "2022-03-06",
  "starting_capital": 1000
}'
echo
