from pybit.unified_trading import HTTP
import pandas as pd
import time
import sqlite3
import requests 
import json 
import datetime as dt
import time
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

class BybitDataFetcher:
    def __init__(self, exchange, symbol, time_horizon, start_date, end_date):

        self.api_key="7nSCb7NvaHogh40lKy"
        self.api_secret="TnpKWNaNWHGqODZY18CmO4nKaOcMdGyHmCaw"

        self.client = HTTP(
            testnet=False,
            api_key=self.api_key,
            api_secret=self.api_secret
        )

        self.symbol = symbol.upper()

        self.time_horizon = time_horizon.upper()
        self.exchange = exchange

        self.start_date = start_date

        self.end_date = end_date
        
        self.db_name = r"db\data.db"
        self.table_name = self.exchange + "_" + self.symbol.lower() + "_" + self.time_horizon.lower()
        
    def _get_interval(self):
        # Translate common names to Bybit intervals
        intervals = {
            "1MINUTE": "1",
            "3MINUTE": "3",
            "5MINUTE": "5",
            "15MINUTE": "15",
            "30MINUTE": "30",
            "1HOUR": "60",
            "4HOUR": "240",
            "DAILY": "D"
        }
        return intervals.get(self.time_horizon.upper(), "60")


    def get_bybit_bars(self, symbol, interval, startTime, endTime):
 
        url = "https://api.bybit.com/v5/market/kline"
    
        startTime = str(int(startTime.timestamp()))
        endTime   = str(int(endTime.timestamp()))
    
        req_params = {"symbol" : symbol, 'interval' : interval, 'from' : startTime, 'to' : endTime}
    
        df = pd.DataFrame(json.loads(requests.get(url, params = req_params).text)['result'])
    
        if (len(df.index) == 0):
            return None
        df["datetime"] = pd.to_datetime(df["startTime"], unit="ms")
        df.set_index("datetime", inplace=True)

        
    
        return df


    def fetch_data(self):
        df_list = []
        last_datetime = self.start_date

        while True:
            print(last_datetime)
            new_df = self.get_bybit_bars(symbol=self.symbol+"USDT", interval=int(self._get_interval()), startTime=last_datetime, endTime=dt.datetime.now())
            if new_df is None:
                break
            df_list.append(new_df)
            last_datetime = max(new_df.index) + dt.timedelta(0, 1)
        
        df = pd.concat(df_list)

        df = pd.DataFrame(columns=[
            "timestamp", "open", "high", "low", "close", "volume", "turnover"
        ])

        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
        df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)
        df = df[["datetime", "open", "high", "low", "close", "volume"]]
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float).round(2)

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
    