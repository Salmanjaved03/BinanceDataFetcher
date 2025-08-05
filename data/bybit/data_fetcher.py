import os
import sys
import time
import pandas as pd
import configparser
from datetime import datetime, timezone
from sqlalchemy import create_engine, text

from pybit.unified_trading import HTTP  # Bybit official SDK

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


class BybitDataFetcher:
    def __init__(self):
        config = configparser.ConfigParser()
        config_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "data", "bybit", "config.ini"
        )
        if not os.path.exists(config_path):
            raise FileNotFoundError(
                f"Config file not found at: {os.path.abspath(config_path)}"
            )
        config.read(config_path)

        if "DATA" not in config:
            raise KeyError("Section 'DATA' not found in config.ini")

        self.symbol = config["DATA"]["symbol"].strip().upper()  # e.g., BTC/USDT
        self.exchange_name = config["DATA"]["exchange"]
        self.time_horizon = config["DATA"]["time_horizon"]
        self.start_date = config["DATA"]["start_date"]
        self.end_date = config["DATA"]["end_date"]
        self.market_type = config["DATA"].get("market_type", "future")

        # Validate ISO 8601 dates
        self._validate_dates()

        # Setup Bybit testnet session
        self.session = HTTP(testnet=False, api_key="", api_secret="")

    def fetch_recent_data(
        self,
        db_params: dict,
        schema: str = "bybit_data",
        table_name: str = "btc_usdt_1min",
    ) -> pd.DataFrame:
        # Connect to DB and get latest timestamp
        db_url = (
            f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}"
            f"@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
        )
        engine = create_engine(db_url)

        with engine.connect() as conn:
            result = conn.execute(
                text(
                    f"""
                    SELECT MAX(datetime) FROM {schema}.{table_name}
                """
                )
            )
            last_dt = result.scalar()

        if last_dt is None:
            print("⚠️ No previous data found. Falling back to config start_date.")
            self._validate_dates()  # fallback to config start/end
            return self.fetch_bybit_ohlcv()

        # Update start_ms to last timestamp + 1 interval
        interval_ms = self.interval_to_milliseconds(self.time_horizon)
        self.start_ms = int(pd.Timestamp(last_dt).timestamp() * 1000) + interval_ms
        self.end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        print(
            f"Fetching new data from: {pd.to_datetime(self.start_ms, unit='ms', utc=True)}"
        )
        return self.fetch_bybit_ohlcv()

    def _validate_dates(self):
        try:
            if not self.start_date:
                raise ValueError("start_date is empty in config.ini")
            self.start_ms = int(
                datetime.fromisoformat(
                    self.start_date.replace("Z", "+00:00")
                ).timestamp()
                * 1000
            )
        except Exception as e:
            raise ValueError(f"Invalid start_date format: {e}")

        if self.end_date.lower() == "now":
            self.end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        else:
            try:
                self.end_ms = int(
                    datetime.fromisoformat(
                        self.end_date.replace("Z", "+00:00")
                    ).timestamp()
                    * 1000
                )
            except Exception as e:
                raise ValueError(f"Invalid end_date format: {e}")

    def interval_to_milliseconds(self, interval):
        # Supports m, h, d
        unit = interval[-1]
        amount = int(interval[:-1])
        if unit == "m":
            return amount * 60 * 1000
        elif unit == "h":
            return amount * 60 * 60 * 1000
        elif unit == "d":
            return amount * 24 * 60 * 60 * 1000
        else:
            raise ValueError(f"Unsupported interval unit: {unit}")

    def fetch_bybit_ohlcv(self, limit=1000) -> pd.DataFrame:
        category_map = {
            "spot": "spot",
            "future": "linear",  # USDT perpetual
            "linear": "linear",
            "inverse": "inverse",
        }
        category = category_map.get(self.market_type.lower(), "linear")
        symbol_formatted = self.symbol.replace("/", "")
        all_data = []
        start_ms = self.start_ms
        interval_ms = self.interval_to_milliseconds(self.time_horizon)
        print(
            f"Fetching {symbol_formatted} on {category} from {self.start_date} to {self.end_date}..."
        )

        while start_ms < self.end_ms:
            try:
                response = self.session.get_kline(
                    category=category,
                    symbol=symbol_formatted,
                    interval=self.time_horizon.replace("m", ""),
                    start=start_ms,
                    limit=limit,
                )
                candles = response["result"]["list"]
                if not candles:
                    print("No more data returned by API.")
                    break

                candles_sorted = sorted(candles, key=lambda x: int(x[0]))
                for c in candles_sorted:
                    timestamp = int(c[0])
                    if timestamp > self.end_ms:
                        break
                    all_data.append(
                        [
                            timestamp,
                            float(c[1]),  # open
                            float(c[2]),  # high
                            float(c[3]),  # low
                            float(c[4]),  # close
                            float(c[5]),  # volume
                        ]
                    )

                last_ts = int(candles_sorted[-1][0])
                print(
                    f"Fetched {len(candles_sorted)} rows up to {pd.to_datetime(last_ts, unit='ms', utc=True)}"
                )
                start_ms = last_ts + interval_ms
                time.sleep(0.2)

            except Exception as e:
                print(f"❌ Error fetching OHLCV: {e}")
                time.sleep(1)
                continue

        if not all_data:
            print("No OHLCV data retrieved.")
            return pd.DataFrame()

        df = pd.DataFrame(
            all_data, columns=["timestamp", "open", "high", "low", "close", "volume"]
        )
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        return df

    def interval_to_milliseconds(self, interval):
        unit = interval[-1]
        amount = int(interval[:-1])
        if unit == "m":
            return amount * 60 * 1000
        elif unit == "h":
            return amount * 60 * 60 * 1000
        elif unit == "d":
            return amount * 24 * 60 * 60 * 1000
        else:
            raise ValueError(f"Unsupported interval unit: {unit}")

    def store_to_postgresql(
        self, df: pd.DataFrame, db_params: dict, schema: str = "bybit_data"
    ):
        if df.empty:
            print("No data to store in PostgreSQL.")
            return

        table_name = "btc_usdt_1min"
        db_url = (
            f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}"
            f"@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
        )
        engine = create_engine(db_url)
        df_to_store = df[["datetime", "open", "high", "low", "close", "volume"]].copy()
        df_to_store["datetime"] = pd.to_datetime(df_to_store["datetime"], utc=True)

        with engine.connect() as conn:
            # Create schema and table if not exists
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            conn.execute(
                text(
                    f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                    datetime TIMESTAMPTZ PRIMARY KEY,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume DOUBLE PRECISION
                )
            """
                )
            )

            # Get last stored datetime
            result = conn.execute(
                text(f"SELECT MAX(datetime) FROM {schema}.{table_name}")
            )
            last_dt = result.scalar()

        if last_dt:
            df_to_store = df_to_store[df_to_store["datetime"] > last_dt]

        if df_to_store.empty:
            print("No new candles to insert.")
            return

        # Append only new rows
        df_to_store.to_sql(
            table_name,
            engine,
            schema=schema,
            if_exists="append",
            index=False,
            method="multi",
        )
        print(f"✅ Appended {len(df_to_store)} new candles to {schema}.{table_name}")


if __name__ == "__main__":
    db_params = {
        "dbname": "postgres",
        "user": "postgres",
        "password": "123abc",
        "host": "localhost",
        "port": 5432,
    }

    try:
        dataFetcher = BybitDataFetcher()
        print("📥 Fetching latest OHLCV data...")
        ohlcv_df = dataFetcher.fetch_recent_data(
            db_params, schema="bybit_data", table_name="btc_usdt_1min"
        )
        print("💾 Storing to PostgreSQL...")
        dataFetcher.store_to_postgresql(ohlcv_df, db_params)
    except Exception as e:
        print(f"❌ Error in main execution: {e}")
