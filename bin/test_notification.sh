#!/bin/bash

# NEED TO HAVE STARTED THE DATABASES AND API SERVER #

echo "Adding AAPL to the email manager"
curl -X 'PUT' \
  'http://127.0.0.1:8000/tickers?ticker=AAPL' \
  -H 'accept: application/json'
echo

echo "Listing all tickers (ensure AAPL is in there)"
curl -X 'GET' \
  'http://127.0.0.1:8000/tickers' \
  -H 'accept: application/json'
echo

echo "Adding test email as an AAPL subscriber"
curl -X 'PUT' \
  'http://127.0.0.1:8000/tickers/AAPL' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '[
  "algotrading.consumer@gmail.com"
]'
echo

echo "List all users for AAPL"
curl -X 'GET' \
  'http://localhost:8000/tickers/AAPL?test=false' \
  -H 'accept: application/json'
echo

echo "Sending an email to the users subscribed to AAPL"
curl -X 'POST' \
  'http://127.0.0.1:8000/tickers/AAPL' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "signal": "BUY",
  "strategy": "SMACross",
  "date": "2022-02-26",
  "test": false
}'
