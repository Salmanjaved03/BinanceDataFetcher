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
        self.db_name = r"C:\Users\321ms\Desktop\Binance\db\BinanceData.db"
        self.table_name = "binance"+ "_" + self.symbol.lower() + "_" + self.time_horizon.lower()

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
    
    def resample_data(self, rule='H'):
        """
        Resample minute-level OHLCV data into larger timeframes.

        Parameters:
        - df: pd.DataFrame with columns ['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume']
            'DateTime' must be datetime64 and set as index or convertible.
        - rule: pandas offset alias string e.g. 'H' for hourly, 'D' for daily, '15T' for 15 minutes

        Returns:
        - resampled DataFrame with OHLCV aggregated properly, rounded to 2 decimals.
        """
        # Ensure 'DateTime' is datetime and set as index
        df = self.load_data_from_db()
        df = df.copy()
        if not pd.api.types.is_datetime64_any_dtype(df['datetime']):
            df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
        df.set_index('datetime', inplace=True)

        ohlc_dict = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }

        resampled = df.resample(rule).agg(ohlc_dict)

        # Drop intervals with no data (e.g., last incomplete interval)
        resampled.dropna(subset=['open', 'close'], inplace=True)

        # Round prices to 2 decimals
        for col in ['open', 'high', 'low', 'close']:
            resampled[col] = resampled[col].round(2)

        # Round volume (if you want, or keep raw)
        resampled['volume'] = resampled['volume'].round(6)  # or int, or leave as is

        # Reset index to bring 'DateTime' back as column
        resampled = resampled.reset_index()

        return resampled

if __name__ == "__main__":
    fetcher = BinanceDataFetcher(symbol="btc", time_horizon="1MINUTE", start_date=datetime(2024, 7, 9), end_date="now")
    #df = fetcher.fetch_data()
    df2 = fetcher.resample_data('H')
    df2.to_csv("abc.csv")