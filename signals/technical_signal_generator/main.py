import configparser
from datetime import datetime, timezone
import os
import sys
import psycopg2
from sqlalchemy import create_engine, inspect, text
import pandas as pd

# Add subfolder to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))

from DataDownloader.download_data import DataDownloader
from indicators.get_indicators import generateIndicators
from signals.technical_signal_generator.technical_signals import (
    getSignals,
    get_last_signal_timestamp,
)


def main():
    db_params = {
        "dbname": "postgres",
        "user": "postgres",
        "password": "123abc",
        "host": "localhost",
        "port": 5432,
    }

    config = configparser.ConfigParser()
    config.read(r"signals/technical_signal_generator/config.ini")

    exchange = "bybit"
    symbol = "btc"
    base_timeframe = "1m"
    resample_timeframe = "1h"

    start_date = datetime.strptime(config["DATA"]["start_date"], "%Y-%m-%d")
    end_date_raw = config["DATA"]["end_date"]
    end_date = (
        datetime.now(timezone.utc)
        if end_date_raw.lower() == "now"
        else datetime.strptime(end_date_raw, "%Y-%m-%d")
    )

    schema_indicators = "indicators"
    schema_signals = "signals"
    indicator_table = f"{schema_indicators}.bybit_btc_1h"
    signal_table_name = f"{exchange}_{symbol}_{resample_timeframe}_signals"
    signal_table_full = f"{schema_signals}.{signal_table_name}"

    try:
        engine = create_engine(
            f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
        )

        inspector = inspect(engine)
        signal_table_exists = inspector.has_table(
            signal_table_name, schema=schema_signals
        )

        with psycopg2.connect(**db_params) as conn_pg:
            with conn_pg.cursor() as cursor:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_signals};")
                conn_pg.commit()

            if signal_table_exists:
                last_dt = get_last_signal_timestamp(engine, signal_table_full)
                print(f"📌 Last signal timestamp: {last_dt}")
                query = f"""
                    SELECT * FROM {indicator_table}
                    WHERE datetime > %s AND datetime <= %s
                    ORDER BY datetime
                """
                df = pd.read_sql_query(query, conn_pg, params=(last_dt, end_date))
            else:
                print("📌 Signal table not found. Fetching full indicator data.")
                query = f"""
                    SELECT * FROM {indicator_table}
                    WHERE datetime BETWEEN %s AND %s
                    ORDER BY datetime
                """
                df = pd.read_sql_query(query, conn_pg, params=(start_date, end_date))

        if df.empty:
            raise ValueError(
                f"No indicator data found in {indicator_table} between {start_date} and {end_date}"
            )

        # Normalize datetime
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_localize(None)
        datetime_col = df["datetime"].copy()

        # Generate signals
        df_signals = getSignals(df.copy())

        if df_signals is None or df_signals.empty:
            raise ValueError("⚠️ No signals were generated.")

        # Ensure datetime exists
        if "datetime" not in df_signals.columns:
            df_signals["datetime"] = datetime_col
        else:
            df_signals["datetime"] = pd.to_datetime(
                df_signals["datetime"], utc=True
            ).dt.tz_localize(None)

        # Drop duplicates
        df_signals.drop_duplicates(subset="datetime", keep="last", inplace=True)

        df_signals.fillna(0, inplace=True)

        # Sort and reset index
        df_signals.sort_values("datetime", inplace=True)
        df_signals.reset_index(drop=True, inplace=True)

        # Write to DB
        with engine.begin() as conn:
            if not signal_table_exists:
                df_signals.to_sql(
                    name=signal_table_name,
                    con=conn,
                    schema=schema_signals,
                    index=False,
                    if_exists="replace",
                    method="multi",
                    chunksize=1000,
                )
                # Add unique constraint to avoid future duplicates
                with conn.connection.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT 1
                        FROM information_schema.table_constraints
                        WHERE constraint_type = 'UNIQUE'
                        AND table_schema = %s
                        AND table_name = %s
                        AND constraint_name = %s;
                    """,
                        (schema_signals, signal_table_name, "unique_datetime"),
                    )

                    constraint_exists = cur.fetchone()

                    if not constraint_exists:
                        cur.execute(
                            f"""
                            ALTER TABLE {signal_table_full}
                            ADD CONSTRAINT unique_datetime UNIQUE(datetime);
                        """
                        )
                        print("✅ Unique constraint added on 'datetime'")
                    conn.connection.commit()
            else:
                # Use temporary table for deduplication
                temp_table = "temp_signal_insert"
                df_signals.to_sql(
                    name=temp_table,
                    con=conn,
                    schema=schema_signals,
                    index=False,
                    if_exists="replace",
                    method="multi",
                    chunksize=1000,
                )
                insert_sql = f"""
                    INSERT INTO {signal_table_full}
                    SELECT * FROM {schema_signals}.{temp_table}
                    ON CONFLICT (datetime) DO NOTHING;
                    DROP TABLE {schema_signals}.{temp_table};
                """
                conn.execute(text(insert_sql))

        print(f"✅ Saved {len(df_signals)} signals to {signal_table_full}")
        print(f"📆 First signal at: {df_signals['datetime'].min()}")
        print(f"📆 Last signal at: {df_signals['datetime'].max()}")

    except Exception as e:
        print(f"❌ Error: {e}")
        raise


if __name__ == "__main__":
    main()
