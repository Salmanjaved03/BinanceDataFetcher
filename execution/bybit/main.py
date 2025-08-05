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
position = None
entry_price = 0.0
TP_PCT = 0.005  # 0.5%
SL_PCT = 0.002  # 0.2%


def get_latest_signal():
    try:
        # Step 1: Run strategy generation script
        print("🧠 Running strategies.py...")
        subprocess.run(
            ["python", "strategies/strategies_pipeline/strategies.py"], check=True
        )

        # Step 2: Connect to PostgreSQL and read latest signal
        print("🔌 Connecting to PostgreSQL...")
        engine = create_engine(
            "postgresql+psycopg2://postgres:123abc@localhost:5432/postgres"
        )

        query = """
        SELECT datetime, signal
        FROM strategy_signal.strategy_1
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


def get_market_price():
    try:
        response = session.get_tickers(category="linear", symbol=symbol)
        price = float(response["result"]["list"][0]["lastPrice"])
        return price
    except Exception as e:
        print("❌ Error getting market price:", e)
        return None


def manage_trade(signal):
    global position, entry_price

    current_price = get_market_price()
    if current_price is None:
        return

    if signal == 1 and position != "long":
        place_order("Buy")
        position = "long"
        entry_price = current_price
        print(f"📈 Entered LONG at {entry_price}")

    elif signal == -1 and position != "short":
        place_order("Sell")
        position = "short"
        entry_price = current_price
        print(f"📉 Entered SHORT at {entry_price}")

    elif signal == 0 and position:
        # Check TP/SL for existing position
        if position == "long":
            if current_price >= entry_price * (1 + TP_PCT):
                print("💰 TP hit. Closing LONG.")
                place_order("Sell")
                position = None
            elif current_price <= entry_price * (1 - SL_PCT):
                print("🔻 SL hit. Closing LONG.")
                place_order("Sell")
                position = None

        elif position == "short":
            if current_price <= entry_price * (1 - TP_PCT):
                print("💰 TP hit. Closing SHORT.")
                place_order("Buy")
                position = None
            elif current_price >= entry_price * (1 + SL_PCT):
                print("🔻 SL hit. Closing SHORT.")
                place_order("Buy")
                position = None
        else:
            print("⚪ No action. Signal = 0 and no position.")


def main():

    while True:
        signal = get_latest_signal()
        manage_trade(signal)

        time.sleep(60)


if __name__ == "__main__":
    main()
