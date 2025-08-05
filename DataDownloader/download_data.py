import os
import sys
from binance.client import Client
from datetime import datetime, timezone, timedelta
import pandas as pd
import sqlite3
from sklearn.preprocessing import (
    StandardScaler,
    MinMaxScaler,
    OrdinalEncoder,
    OneHotEncoder,
)
from sklearn.impute import SimpleImputer
import numpy as np

sys.path.append(os.path.join(os.path.dirname(__file__), "data"))

from data.binance.data_fetcher import BinanceDataFetcher
from data.bybit.data_fetcher import BybitDataFetcher


class DataDownloader:
    def __init__(self, exchange, symbol, time_horizon, start_date, end_date):
        self.symbol = symbol
        self.exchange = exchange
        self.start_date = start_date
        self.end_date = end_date
        self.time_horizon = time_horizon

    def resample_data(self, rule):
        """
        Resample minute-level OHLCV data into larger timeframes.
        Automatically fetches data if not found in DB.

        Parameters:
        - rule: pandas offset alias string e.g. 'H' for hourly, 'D' for daily, '15T' for 15 minutes

        Returns:
        - pd.DataFrame: Resampled OHLCV DataFrame
        """

        if self.exchange == "binance":
            fetcher = BinanceDataFetcher(
                exchange=self.exchange,
                symbol=self.symbol,
                time_horizon=self.time_horizon,
                start_date=self.start_date,
                end_date=self.end_date,
            )
        elif self.exchange == "bybit":
            fetcher = BybitDataFetcher(
                exchange=self.exchange,
                symbol=self.symbol,
                time_horizon=self.time_horizon,
                start_date=self.start_date,
                end_date=self.end_date,
            )

        # Check if the table exists
        conn = sqlite3.connect(fetcher.db_name)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
            (fetcher.table_name,),
        )
        table_exists = cursor.fetchone()
        conn.close()

        # If table does not exist, fetch data
        if not table_exists:
            print(
                f"[INFO] Table '{fetcher.table_name}' not found in DB. Fetching data from '{fetcher.exchange}' API..."
            )
            fetcher.fetch_data()

        # Load from DB
        df = fetcher.load_data_from_db().copy()

        # Ensure datetime column is datetime type
        if not pd.api.types.is_datetime64_any_dtype(df["datetime"]):
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True)

        df.set_index("datetime", inplace=True)

        ohlc_dict = {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }

        resampled = df.resample(rule).agg(ohlc_dict)
        resampled.dropna(subset=["open", "close"], inplace=True)

        for col in ["open", "high", "low", "close", "volume"]:
            resampled[col] = resampled[col].round(2)

        return resampled.reset_index()

    def preprocessing(self, df):

        if df.isnull().values.any():
            imputer = SimpleImputer(missing_values=np.nan, strategy="mean")
            df[df.columns] = imputer.fit_transform(df)

        return df.reset_index(drop=True)

    def fetch_resampled(self, rule):
        resampled_df = self.resample_data(rule)
        start = pd.to_datetime(self.start_date, utc=True)
        end = pd.to_datetime(
            datetime.utcnow() if self.end_date == "now" else self.end_date, utc=True
        )
        filtered_df = resampled_df[
            (resampled_df["datetime"] >= start) & (resampled_df["datetime"] <= end)
        ]
        final_df = self.preprocessing(filtered_df)
        return final_df.reset_index(drop=True)
