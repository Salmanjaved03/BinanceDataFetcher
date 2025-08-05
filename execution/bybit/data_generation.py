import time
import pandas as pd
from datetime import datetime, timezone
from pybit.unified_trading import HTTP
import subprocess
import sys
import os
import pandas as pd
from sqlalchemy import create_engine

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))

# Bybit session config
session = HTTP(
    api_key="741OEL87G21NCofK2d",
    api_secret="6zKO3b77mxd9cIHEIj994O2WsxxWxJ1k1P3Z",
    demo=True,
    recv_window=10000,
)

symbol = "BTCUSDT"
qty = 0.001
position = None  # Track current position (None, "long", "short")


def fetch_data():
    print("📥 Fetching market data...")
    subprocess.run(["python", "data/bybit/data_fetcher.py"])


def generate_signals():
    print("📊 Generating indicators and signals...")
    subprocess.run(["python", "indicators/get_indicators.py"])
    subprocess.run(["python", "signals/technical_signal_generator/main.py"])


def get_latest_signal():
    try:
        # Step 1: Run strategy generation script
        print("🧠 Running strategies.py...")
        subprocess.run(["python", "strategies.py"], check=True)

        # Step 2: Connect to PostgreSQL and read latest signal
        print("🔌 Connecting to PostgreSQL...")
        engine = create_engine(
            "postgresql+psycopg2://postgres:123abc@localhost:5432/postgres"
        )

        query = """
        SELECT datetime, signal
        FROM signal.strategy_1
        ORDER BY datetime DESC
        LIMIT 1;
        """
        df = pd.read_sql_query(query, engine)

        if df.empty:
            print("⚠️ No signals found in strategy_1 table.")
            return 0

        latest_signal = int(df.iloc[0]["signal"])
        print(f"📈 Latest signal: {latest_signal}")
        return latest_signal

    except Exception as e:
        print("⚠️ Error getting signal from DB:", e)
        return 0


def place_order(side):
    try:
        print(f"🛒 Placing {side} order")
        order = session.place_order(
            category="linear",
            symbol=symbol,
            side=side,
            order_type="Market",
            qty=str(qty),
            time_in_force="GoodTillCancel",
            reduce_only=False,
            close_on_trigger=False,
        )
        print("✅ Order placed:", order)
    except Exception as e:
        print("❌ Error placing order:", e)


def manage_trade(signal):
    global position
    if signal == 1 and position != "long":
        place_order("Buy")
        position = "long"
    elif signal == -1 and position != "short":
        place_order("Sell")
        position = "short"
    elif signal == 0:
        print("⚪ No action. Signal = 0")


def main():
    last_signal_run = -1

    while True:
        now = datetime.now(timezone.utc)
        minute = now.minute
        hour = now.hour

        fetch_data()

        if now.minute == 0 and last_signal_run != hour:
            generate_signals()
            last_signal_run = hour

            signal = get_latest_signal()

        time.sleep(60)


if __name__ == "__main__":
    main()
