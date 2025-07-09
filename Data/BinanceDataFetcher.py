import os
from binance.client import Client
import datetime
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
            'Open Time', 'Open', 'High', 'Low', 'Close', 'Volume',
            'Close Time', 'Quote Asset Volume', 'Number of trades',
            'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore'
        ]
        data = pd.DataFrame(historical_data, columns=columns)

        data['Open Time UTC'] = pd.to_datetime(data['Open Time'], unit='ms', utc=True)
        data['Close Time UTC'] = pd.to_datetime(data['Close Time'], unit='ms', utc=True)
        data['Open Time'] = data['Open Time UTC']
        data['Close Time'] = data['Close Time UTC']

        df = data.drop(columns=[
            'Volume', 'Close Time', 'Quote Asset Volume', 'Number of trades',
            'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore',
            'Open Time UTC', 'Close Time UTC'
        ])

        if self.end_date == "now":
            df.drop(df.index[-1], inplace=True)

        db_name = "BinanceData.db"
        conn = sqlite3.connect(db_name)

        table_name = self.symbol.upper() + "_" + self.time_horizon + "_" + str(self.start_date) + "_" + str(self.end_date)
        df.to_sql(table_name, conn, if_exists='replace', index=False)

        return df

if __name__ == "__main__":
    fetcher = BinanceDataFetcher(symbol="doge", time_horizon="1DAY", start_date=datetime.datetime(2025, 7, 5), end_date="now")
    df = fetcher.fetch_data()