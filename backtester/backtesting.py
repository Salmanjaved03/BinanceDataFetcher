import pandas as pd
import psycopg2
import os
import sys
from sqlalchemy import create_engine, text
import optuna

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

db_params = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "123abc",
    "host": "localhost",
    "port": 5432,
}


def backtest_strategy(
    df: pd.DataFrame, tp=0.05, sl=0.03, initial_balance=1000, fee_rate=0.0005
):
    df = df.copy()
    df.reset_index(drop=True, inplace=True)

    position = None
    entry_price = 0.0
    entry_position_size = 0.0  # Track entry position size
    balance = initial_balance
    trades = []
    pnlSum = 0.0
    last_signal = 0  # Prevent re-entry on same signal

    for i in range(len(df)):
        row = df.iloc[i]
        signal = row["signal"]
        price = row["open"]
        high = row["high"]
        low = row["low"]

        exit_price = None
        action_label = ""

        # === Exit logic ===
        if position == "long":
            tp_price = entry_price * (1 + tp)
            sl_price = entry_price * (1 - sl)

            if high >= tp_price:
                exit_price = high
                action_label = "sell-tp"
            elif low <= sl_price:
                exit_price = low
                action_label = "sell-sl"
            elif signal == -1:
                exit_price = price
                action_label = "change_direction"

        elif position == "short":
            tp_price = entry_price * (1 - tp)
            sl_price = entry_price * (1 + sl)

            if low <= tp_price:
                exit_price = low
                action_label = "cover-tp"
            elif high >= sl_price:
                exit_price = high
                action_label = "cover-sl"
            elif signal == 1:
                exit_price = price
                action_label = "change_direction"

        # === Handle exit ===
        if exit_price is not None:
            position_size = entry_position_size  # Use entry position size
            if position == "long":
                pnl_pct = (exit_price - entry_price) / entry_price
            else:
                pnl_pct = (entry_price - exit_price) / entry_price

            pnl = position_size * pnl_pct
            fee = position_size * fee_rate  # Fee based on entry position
            pnl -= fee

            balance += pnl
            pnlSum += pnl_pct * 100

            trades.append(
                {
                    "datetime": row["datetime"],
                    "action": action_label,
                    "buy_price": entry_price,
                    "sell_price": exit_price,
                    "pnl_percent": round(pnl_pct * 100, 4),
                    "pnl_sum": round(pnlSum, 4),
                    "balance": round(balance, 2),
                }
            )

            position = None
            entry_price = 0.0
            entry_position_size = 0.0
            last_signal = 0

        # === Entry logic ===
        if position is None and signal != 0 and signal != last_signal:
            position = "long" if signal == 1 else "short"
            entry_price = price
            entry_position_size = balance  # Store entry position size
            fee = entry_position_size * fee_rate
            balance -= fee
            pnlSum -= fee_rate * 100

            trades.append(
                {
                    "datetime": row["datetime"],
                    "action": "buy" if signal == 1 else "short",
                    "buy_price": entry_price,
                    "sell_price": 0.0,
                    "pnl_percent": 0.0,
                    "pnl_sum": round(pnlSum, 4),
                    "balance": round(balance, 2),
                }
            )

            last_signal = signal

    # === Close open position at last row ===
    if position is not None:
        row = df.iloc[-1]
        exit_price = row["open"]
        position_size = entry_position_size
        if position == "long":
            pnl_pct = (exit_price - entry_price) / entry_price
            action_label = "SELL_CLOSE"
        else:
            pnl_pct = (entry_price - exit_price) / entry_price
            action_label = "COVER_CLOSE"

        pnl = position_size * pnl_pct
        fee = position_size * fee_rate
        pnl -= fee

        balance += pnl
        pnlSum += pnl_pct * 100

        trades.append(
            {
                "datetime": row["datetime"],
                "action": action_label,
                "buy_price": entry_price,
                "sell_price": exit_price,
                "pnl_percent": round(pnl_pct * 100, 4),
                "pnl_sum": round(pnlSum, 4),
                "balance": round(balance, 2),
            }
        )

    return pd.DataFrame(trades)


def merge_df(ohlcv_df: pd.DataFrame, signal_df: pd.DataFrame):
    merged = pd.merge(ohlcv_df, signal_df, on="datetime", how="left")
    merged["signal"] = merged["signal"].fillna(0).astype(int)
    return merged


def run_backtests_on_all_strategies(db_params: dict, ohlcv_df: pd.DataFrame):
    from sqlalchemy import create_engine, ස

    db_url = f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    engine = create_engine(db_url)

    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'strategy_signal' AND table_name ~ '^strategy_[0-9]+$'
            """
            )
        )
        tables = [row[0] for row in result.fetchall()]

        for table in tables:
            full_table = f"strategy_signal.{table}"

            # Load signals and OHLCV
            signal_df = pd.read_sql_query(f"SELECT * FROM {full_table}", con=engine)
            if signal_df["datetime"].dt.tz is None:
                signal_df["datetime"] = (
                    signal_df["datetime"]
                    .dt.tz_localize("Asia/Karachi")
                    .dt.tz_convert("UTC")
                )
            else:
                signal_df["datetime"] = signal_df["datetime"].dt.tz_convert("UTC")
            ohlcv_df["datetime"] = pd.to_datetime(ohlcv_df["datetime"], utc=True)

            # Merge with left join and fill missing signals with 0
            merged = pd.merge(ohlcv_df, signal_df, on="datetime", how="left")
            merged["signal"] = merged["signal"].fillna(0).astype(int)

            if merged.empty:
                continue

            result_df = backtest_strategy(merged)
            result_df["pnl_percent"] = result_df["pnl_percent"].astype(float)

            backtest_table = f"backtest_{table}"
            result_df.to_sql(
                name=backtest_table,
                con=engine,
                schema="strategy_backtest",
                if_exists="replace",
                index=False,
                method="multi",
            )


def objective(trial, ohlcv_df: pd.DataFrame, signal_df: pd.DataFrame):
    # Suggest hyperparameters to optimize
    tp = trial.suggest_float("tp", 0.01, 0.05)  # take profit: 1% to 20%
    sl = trial.suggest_float("sl", 0.01, 0.03)  # stop loss: 1% to 20%
    fee = trial.suggest_float("fee_rate", 0.000, 0.0005)

    # Run backtest with these hyperparameters
    merged_df = merge_df(ohlcv_df=ohlcv_df, signal_df=signal_df)
    result_df = backtest_strategy(merged_df, tp=tp, sl=sl, fee_rate=fee)

    if result_df.empty:
        return -999999  # Penalize empty results

    # Use final balance as optimization target
    final_balance = result_df["balance"].iloc[-1]

    return final_balance  # Maximize this
