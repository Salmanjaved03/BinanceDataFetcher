import os
import sys
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

# Setup path
sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))

# Database configuration
config = {
    "drivername": "postgresql+psycopg2",
    "username": "postgres",
    "password": "123abc",
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
}


def get_engine():
    return create_engine(URL.create(**config))


def get_strategy_row(strategy_name):
    engine = get_engine()
    query = f"SELECT * FROM public.strategies WHERE strategy_name = '{strategy_name}'"
    df = pd.read_sql(query, engine)
    if df.empty:
        raise ValueError(
            f"❌ Strategy '{strategy_name}' not found in strategies table."
        )
    return df.iloc[0]


def get_signal_dataframe(table_name):
    engine = get_engine()
    query = f"SELECT * FROM signals.{table_name}"
    df = pd.read_sql(query, engine)
    print(f"✔️  Loaded {len(df)} rows from signals.{table_name}")
    return df


def extract_strategy_signals(strategy_row):
    indicators = [
        f"sig_{col}"
        for col in strategy_row.index
        if col not in ["strategy_name", "exchange", "symbol"] and strategy_row[col]
    ]
    print(
        f"✔️  Strategy '{strategy_row['strategy_name']}' uses indicators: {indicators}"
    )
    return indicators


def generate_final_signals(df, signal_columns):
    valid_columns = [col for col in signal_columns if col in df.columns]

    if not valid_columns:
        print("❌ No matching signal columns found in DataFrame.")
        df["signal"] = 0
    else:
        print("✔️ Signal columns used for voting:", valid_columns)
        vote_sum = df[valid_columns].fillna(0).sum(axis=1)
        df["signal"] = np.sign(vote_sum).astype(int)

    print("🧾 Signal value counts:")
    print(df["signal"].value_counts())

    return df[["datetime", "signal"]].reset_index(drop=True)


def save_final_signals(df, strategy_name):
    engine = get_engine()
    schema = "strategy_signal"
    output_table = strategy_name

    with engine.begin() as conn:
        df.to_sql(output_table, conn, schema=schema, if_exists="replace", index=False)
        print(f"✅ Final signals saved to {schema}.{output_table} ({len(df)} rows)")


def main():
    strategy_name = "strategy_1"
    signals_table_name = "bybit_btc_1h_signals"

    strategy_row = get_strategy_row(strategy_name)
    signal_df = get_signal_dataframe(signals_table_name)
    indicator_columns = extract_strategy_signals(strategy_row)

    final_df = generate_final_signals(signal_df, indicator_columns)
    save_final_signals(final_df, strategy_name)


if __name__ == "__main__":
    main()
