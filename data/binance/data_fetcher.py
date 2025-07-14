import os
from binance.client import Client
from datetime import datetime, timezone, timedelta
import pandas as pd
import sqlite3
from sklearn.preprocessing import StandardScaler,MinMaxScaler,OrdinalEncoder,OneHotEncoder
from sklearn.impute import SimpleImputer
import numpy as np

class BinanceDataFetcher:
    def __init__(self, exchange, symbol, time_horizon, start_date, end_date):
        self.api_key = os.getenv("BINANCE_API_KEY", "test_api_key")
        self.api_secret = os.getenv("BINANCE_API_SECRET", "test_api_secret")

        self.client = Client(api_key=self.api_key, api_secret=self.api_secret)

        self.symbol = symbol.upper()

        self.time_horizon = time_horizon.upper()
        self.interval = self._get_interval()
        self.exchange = exchange

        self.start_date = str(start_date)
        self.end_date = str(end_date)
        self.db_name = r"C:\Users\321ms\Desktop\Binance\db\data.db"
        self.table_name = self.exchange + "_" + self.symbol.lower() + "_" + self.time_horizon.lower()

    def _get_interval(self):
        try:
            interval = getattr(Client, f"KLINE_INTERVAL_{self.time_horizon}")
            return interval
        except AttributeError:
            raise ValueError(f"Invalid time horizon: {self.time_horizon}")

    def fetch_data(self):
        if self.end_date == "":
            self.end_date = "now"
        historical_data = self.client.get_historical_klines(
            symbol=self.symbol+"USDT",
            interval=self.interval,
            start_str=self.start_date, 
            end_str=self.end_date
        )

        columns = [
            'datetime', 'open', 'high', 'low', 'close', 'volume',
            'Close Time', 'Quote Asset Volume', 'Number of trades',
            'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore'
        ]
        data = pd.DataFrame(historical_data, columns=columns)

        data['Open Time UTC'] = pd.to_datetime(data['datetime'], unit='ms', utc=True)
        data['Close Time UTC'] = pd.to_datetime(data['Close Time'], unit='ms', utc=True)
        data['datetime'] = data['Open Time UTC']
        data['Close Time'] = data['Close Time UTC']
        for col in ['open', 'close', 'high', 'low', 'volume']:
            data[col] = data[col].astype(float).round(2)
        df = data.drop(columns=[
            'Close Time', 'Quote Asset Volume', 'Number of trades',
            'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore',
            'Open Time UTC', 'Close Time UTC'
        ])

        if self.end_date == "now":
            df.drop(df.index[-1], inplace=True)

        conn = sqlite3.connect(self.db_name)

        df.to_sql(self.table_name, conn, if_exists='replace', index=False)

        return df
    
    def load_data_from_db(self):
        """
        Load data from the SQLite table into a DataFrame.
        Returns:
            pd.DataFrame: Loaded DataFrame from the table.
        """
        conn = sqlite3.connect(self.db_name)
        query = f"SELECT * FROM {self.table_name}"
        df = pd.read_sql_query(query, conn)
        conn.close()

        # Ensure datetime column is parsed correctly
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'], utc=True)

        return df
