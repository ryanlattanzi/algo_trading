#!/usr/bin/env bash

# creates a new terminal window
# function new() {
#     if [[ $# -eq 0 ]]; then
#         open -a "Terminal" "$PWD"
#     else
#         open -a "Terminal" "$@"
#     fi
# }

# echo "Build from scratch? (y)"
# read from_scratch

# if [ "$from_scratch" == "y" ]; then
#     echo "Tearing down and building from scratch..."
#     ./startup.sh y
#     sleep 5
#     new
# fi

#Orchestrating the DAGs
cd ./dags
python orchestrate.py

#Backtesting
cd ../back_testing
python sma_cross_backtest.py

#Change dir to original
cd $current_dir