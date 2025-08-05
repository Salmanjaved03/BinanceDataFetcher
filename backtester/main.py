import pandas as pd
import psycopg2
import os
import sys
from sqlalchemy import create_engine, text

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from backtester.backtesting import backtest_strategy, run_backtests_on_all_strategies

db_params = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "123abc",
    "host": "localhost",
    "port": 5432,
}


def main():
    # Fetch OHLCV data from PostgreSQL
    try:
        conn_pg = psycopg2.connect(**db_params)
        ohlcv_query = "SELECT * FROM bybit_data.btc_usdt_1m"
        ohlcv_df = pd.read_sql_query(ohlcv_query, conn_pg)
        conn_pg.close()

        if ohlcv_df.empty:
            raise ValueError("No OHLCV data found in bybit_data.btc_usdt_1m")

        conn_pg = psycopg2.connect(**db_params)
        signal_query = "SELECT * FROM strategy_signal.strategy_0"
        signal_df = pd.read_sql_query(signal_query, conn_pg)
        conn_pg.close()

        # Ensure datetime is in correct format
        if "datetime" in ohlcv_df.columns:
            ohlcv_df["datetime"] = pd.to_datetime(ohlcv_df["datetime"], utc=True)
        else:
            raise ValueError("OHLCV data missing 'datetime' column")

        print(
            f"Fetched {len(ohlcv_df)} OHLCV rows from {ohlcv_df['datetime'].min()} to {ohlcv_df['datetime'].max()}"
        )

    except Exception as e:
        print(f"❌ Error fetching OHLCV data from PostgreSQL: {e}")
        return

    try:
        result = run_backtests_on_all_strategies(
            db_params=db_params, ohlcv_df=ohlcv_df, signal_df=signal_df
        )
        print("✅ Backtesting completed")
    except Exception as e:
        print(f"❌ Error during backtesting: {e}")


if __name__ == "__main__":
    main()

    # Commented out previous SQLite code for testing purposes
    """
    # Previous SQLite code (replaced by PostgreSQL)
    conn_sqlite = sqlite3.connect(r"db\data.db")
    ohlcv_query = "SELECT * FROM binance_btc_1minute"
    ohlcv_df = pd.read_sql_query(ohlcv_query, conn_sqlite)
    conn_sqlite.close()
    """

    # Commented out previous PostgreSQL signal code (replaced by CSV)
    """
    conn_pg = psycopg2.connect(**db_params)
    signal_query = "SELECT * FROM strategy_signal.strategy_0"
    signal_df = pd.read_sql_query(signal_query, conn_pg)
    conn_pg.close()
    """
