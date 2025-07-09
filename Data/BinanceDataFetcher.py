import os
from binance.client import Client
from datetime import datetime, timezone, timedelta
import pandas as pd
import sqlite3


class BinanceDataFetcher:
    def __init__(self, symbol, time_horizon, start_date, end_date):
        self.api_key = os.getenv("BINANCE_API_KEY", "test_api_key")
        self.api_secret = os.getenv("BINANCE_API_SECRET", "test_api_secret")

        self.client = Client(api_key=self.api_key, api_secret=self.api_secret)

        self.symbol = symbol.upper() + "USDT"

        self.time_horizon = time_horizon.upper()
        self.interval = self._get_interval()

        self.start_date = str(start_date)
        self.end_date = str(end_date)
        self.db_name = r"C:\Users\321ms\Desktop\Binance\BinanceData.db"
        self.table_name = "binance"+ "_" + self.symbol.upper() + "_" + self.time_horizon

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
            symbol=self.symbol,
            interval=self.interval,
            start_str=self.start_date, 
            end_str=self.end_date
        )

        columns = [
            'DateTime', 'Open', 'High', 'Low', 'Close', 'Volume',
            'Close Time', 'Quote Asset Volume', 'Number of trades',
            'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore'
        ]
        data = pd.DataFrame(historical_data, columns=columns)

        data['Open Time UTC'] = pd.to_datetime(data['DateTime'], unit='ms', utc=True)
        data['Close Time UTC'] = pd.to_datetime(data['Close Time'], unit='ms', utc=True)
        data['DateTime'] = data['Open Time UTC']
        data['Close Time'] = data['Close Time UTC']
        data['Open'] = round(data['Open'], 2)
        data['Close'] = round(data['Close'], 2)
        data['High'] = round(data['High'], 2)
        data['Low'] = round(data['Low'], 2)
        df = data.drop(columns=[
            'Volume', 'Close Time', 'Quote Asset Volume', 'Number of trades',
            'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore',
            'Open Time UTC', 'Close Time UTC'
        ])

        if self.end_date == "now":
            df.drop(df.index[-1], inplace=True)

        conn = sqlite3.connect(self.db_name)

        df.to_sql(self.table_name, conn, if_exists='replace', index=False)

        return df
    
    def get_entries_before_hours(self, hours_ago, timestamp_column= "DateTime"):
        time_threshold = (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).strftime('%Y-%m-%d %H:%M:%S')
        conn = sqlite3.connect(self.db_name)


        query = f"""
            SELECT *
            FROM {self.table_name}
            WHERE {timestamp_column} >= ?
        """

        df = pd.read_sql_query(query, conn, params=(time_threshold,))

        conn.close()
        return df

if __name__ == "__main__":
    fetcher = BinanceDataFetcher(symbol="btc", time_horizon="1MINUTE", start_date=datetime(2024, 7, 9), end_date="now")
    #df = fetcher.fetch_data()
    df2 = fetcher.get_entries_before_hours(4)
    df2.to_csv("abc.csv")