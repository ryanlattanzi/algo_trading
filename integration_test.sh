
#Orchestrating the DAGs
cd ./dags
python orchestrate.py

#Backtesting
cd ../back_testing
python sma_cross_backtest.py

#Change dir to original
cd $current_dir