import psycopg2 as pg
import psycopg2.extras as pge
import yaml as yl
import datetime


class DbHandler:
    def __init__(self, config_file, stock_dict):
        self.config_file = config_file
        self.stock_dict = stock_dict
        self.c = None

    #Add the host, database, user, password, port into the config.yml file and reference that
    def db_connection(self):
        conn = pg.connect(
            host = "localhost",
            database="dev",
            user="dev",
            password="password",
            port="5432"
        )
        self.c = conn.cursor()
        return conn

    def create_table(self,ticker):
        conn = self.db_connection()
        self.c.execute(f"""CREATE TABLE {ticker} (
            date text,
            open_price real,
            high_price real,
            low_price real,
            close_price real,
            adj_close real,
            volume real)""")
        conn.commit()

    def check_new_tickers(self):
        conn = self.db_connection()
        tickers_yl = yl.safe_load(open(self.config_file, 'r'))
        ticker_list = tickers_yl['ticker_list']
        self.c.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'""")
        tables = self.c.fetchall()
        table_list = [item[0] for item in tables]
        new_ticker_list = list()
        for ticker in ticker_list:
            if ticker not in table_list:
                self.create_table(ticker)
                new_ticker_list.append(ticker)
            conn.commit()
        return new_ticker_list

    def add_hist_data(self, ticker, df):
        conn = self.db_connection()
        for row in df.index:
            open_price = df['Open'][row]
            date = df['Date'][row]
            close_price = df['Close'][row]
            high_price = df['High'][row]
            low_price = df['Low'][row]
            adj_close = df['Adj Close'][row]
            volume = df['Volume'][row]
            print(date)
            query_string = f"""
            INSERT into {ticker}(date, open_price, high_price, low_price, close_price, adj_close, volume)
            values({date}, {open_price}, {high_price}, {low_price}, {close_price}, {adj_close}, {volume})
            """
            self.c.execute(query_string)
            conn.commit()

    def insert_record(self,ticker, time, open_price, close, high, low, volume):
        time_conv = datetime.datetime.fromtimestamp((time/1000)).strftime('%d-%m-%Y %H:%M:%S')
        conn = self.db_connection()
        self.c.execute(f"""INSERT INTO {ticker} VALUES (?,?,?,?,?,?)""", (time_conv, open_price, close, high, low, volume))
        conn.commit()