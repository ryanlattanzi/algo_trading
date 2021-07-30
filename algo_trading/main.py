import yaml as yl
from stock_data import *
from db_handler import *




#Initialize the stock data class
tickers_yl = yl.safe_load(open('config.yml', 'r'))
ticker_list = tickers_yl['ticker_list']
stock_data_dict = dict()

db = DbHandler('config.yml', stock_data_dict)
db.db_connection()
new_ticker_list = db.check_new_tickers()

#This for loop checks to see if a new ticker was added to the config.yml file
for ticker in new_ticker_list:
    stock_init = StockData(ticker, 'max', '07-28-2021', '1d')
    query_string = stock_init.create_query_string()
    stock_data = stock_init.get_stock_data()
    stock_data_dict[ticker] = stock_data

#print(stock_data_dict['aapl'].get('Open'))

#This will loop through the in-memory stock data from the dictionary and add it to the postgres database
for ticker, df in stock_data_dict.items():
    db.add_hist_data(ticker, df)









