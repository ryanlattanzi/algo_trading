import time
import pandas as pd
from dateutil.parser import parse

#Create class that will pull yahoo.com stock data given a symbol, start, end, and trade interval.
class StockData:
    def __init__(self, symbol, start_date, end_date, interval):
        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date
        self.interval = interval

    #Function that will create the query string that will be used to access yahoo stock data
    def create_query_string(self):
        if self.start_date == 'max':
            start_period_in = 0000000000
        else:
            start_date_obj = parse(self.start_date)
            start_period_in = int(time.mktime(start_date_obj.timetuple()))

        end_date_obj = parse(self.end_date)
        end_period_in = int(time.mktime(end_date_obj.timetuple()))
        query_string = f'https://query1.finance.yahoo.com/v7/finance/download/' \
                       f'{self.symbol}?period1={start_period_in}&period2={end_period_in}' \
                       f'&interval={self.interval}&events=history&includeAdjustedClose=true'
        return query_string

    #Function will get stock data from yahoo finance based on the query string
    def get_stock_data(self):
        hist_data = pd.read_csv(self.create_query_string())
        return hist_data

    #Function will look at database and see what last entry date is will get stock data from that last entry to present