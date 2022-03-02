#!/bin/bash

# If we specify a directory, then use that to look
# for the app.

if [ -z "$1" ]
  then
    echo "Running notification app in current working dir."
    uvicorn app:app --reload --port 8000
  else
    echo "Running notification app in supplied dir {$1}"
    uvicorn app:app --reload --port 8000 --app-dir {$1}
fi

sleep 2
