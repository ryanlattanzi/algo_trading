#!/bin/bash

# If we specify a directory, then use that to look
# for the app.

if [ -z "$1" ]
  then
    echo "Running backtester app in current working dir."
    uvicorn app:app --reload --host 0.0.0.0 --port 8005
  else
    echo "Running backtester app in supplied dir {$1}"
    uvicorn app:app --reload --host 0.0.0.0 --port 8005 --app-dir {$1}
fi

sleep 2
