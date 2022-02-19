#!/bin/bash

# If arg "new" is supplied, tear down the env
# and start from scratch.

CONTAINER="algo_trading"
SECONDS=0

echo "Integration test from scratch? (y/n): "
say test that jawns from scratch
read from_scratch

if [ "${from_scratch}" = "y" ]
    then
        echo "Tearing down and building from scratch..."
        say integration testing this bih from scratch

        ./bin/startup.sh new &

        # Waits until pgadmin is up and running, which is the container
        # that takes longest to stand up.
        until docker logs ${CONTAINER}-pgadmin-1 2>&1 | grep -m 1 -e "Booting worker with pid:"
        do
            sleep 1
        done

        echo "waited $SECONDS for env to set up"

fi

#Orchestrating the DAGs
cd ./dags
python orchestrate.py

#Backtesting
cd ../back_testing
python sma_cross_backtest.py
