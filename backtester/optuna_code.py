import pandas as pd
import numpy as np
import optuna
from sqlalchemy import create_engine, text
from optuna.samplers import TPESampler
from datetime import datetime
import sys
import os
from contextlib import contextmanager
from sqlalchemy.pool import QueuePool

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from backtester.backtesting import backtest_strategy

# Configuration
DB_PARAMS = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "123abc",
    "host": "localhost",
    "port": 5432,
}
EXCHANGE = "bybit"
SYMBOL = "btc"
N_TRIALS = 5  # Number of trials to generate 5 different strategies
TP = 0.05
SL = 0.03
FEE_RATE = 0.0005
INITIAL_BALANCE = 1000
MIN_PNL_SUM = 100


def get_db_engine():
    return create_engine(
        f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}",
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=10,
    )


@contextmanager
def get_db_connection(engine):
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()


def ensure_utc_datetime(df, datetime_col="datetime"):
    if not isinstance(df, pd.DataFrame) or df.empty:
        return df
    if df[datetime_col].dt.tz is None:
        df[datetime_col] = pd.to_datetime(df[datetime_col]).dt.tz_localize("UTC")
    else:
        df[datetime_col] = df[datetime_col].dt.tz_convert("UTC")
    return df


def verify_table_exists(engine, schema, table_name):
    with get_db_connection(engine) as conn:
        result = conn.execute(
            text(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = :schema AND table_name = :table_name)"
            ),
            {"schema": schema, "table_name": table_name},
        ).scalar()
    return bool(result)


def get_available_indicators(engine):
    if not verify_table_exists(engine, "signals", "bybit_btc_1h_signals"):
        raise ValueError("Table signals.bybit_btc_1h_signals does not exist")

    with get_db_connection(engine) as conn:
        result = conn.execute(
            text(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = 'signals' AND table_name = 'bybit_btc_1h_signals' AND column_name LIKE 'sig\\_%'"
            )
        )
        indicators = [row[0].replace("sig_", "") for row in result]

    if not indicators:
        raise ValueError("No signal columns found")
    return indicators


def create_strategy_table(engine, indicators):
    with get_db_connection(engine) as conn:
        # Drop the existing strategies table to ensure correct schema
        conn.execute(text("DROP TABLE IF EXISTS public.strategies"))

        # Create new strategies table
        conn.execute(
            text(
                """
                CREATE TABLE public.strategies (
                    strategy_name VARCHAR(50) PRIMARY KEY,
                    exchange VARCHAR(20),
                    symbol VARCHAR(10),
                    final_balance FLOAT,
                    pnl_sum FLOAT,
                    sharpe_ratio FLOAT,
                    total_trades INTEGER,
                    win_rate FLOAT,
                    max_drawdown FLOAT
                )
            """
            )
        )

        # Add a boolean column for each indicator
        for indicator in indicators:
            try:
                conn.execute(
                    text(
                        f"ALTER TABLE public.strategies ADD COLUMN IF NOT EXISTS {indicator} BOOLEAN DEFAULT FALSE"
                    )
                )
            except Exception as e:
                print(f"Could not add column for {indicator}: {str(e)}")

        conn.commit()


def calculate_performance_metrics(results):
    if results.empty or "pnl_sum" not in results.columns:
        return {
            "final_balance": 0,
            "pnl_sum": 0,
            "sharpe_ratio": 0,
            "total_trades": 0,
            "win_rate": 0,
            "max_drawdown": 0,
        }

    final_balance = results["balance"].iloc[-1]
    pnl_sum = results["pnl_sum"].iloc[-1]
    total_trades = len(
        results[results["action"].str.contains("tp|sl|close", case=False, na=False)]
    )
    winning_trades = len(
        results[
            (results["action"].str.contains("tp|sl|close", case=False, na=False))
            & (results["pnl_percent"] > 0)
        ]
    )
    win_rate = winning_trades / total_trades if total_trades > 0 else 0.0

    returns = results["pnl_percent"] / 100
    sharpe_ratio = (
        returns.mean() / returns.std() * np.sqrt(365 * 24)
        if len(returns) > 1 and returns.std() != 0
        else 0.0
    )

    cumulative_returns = (1 + returns).cumprod()
    peak = cumulative_returns.expanding(min_periods=1).max()
    drawdown = (cumulative_returns - peak) / peak
    max_drawdown = drawdown.min() if not drawdown.empty else 0.0

    return {
        "final_balance": final_balance,
        "pnl_sum": pnl_sum,
        "sharpe_ratio": sharpe_ratio,
        "total_trades": total_trades,
        "win_rate": win_rate,
        "max_drawdown": max_drawdown,
    }


def generate_strategy(engine, strategy_num, selected_indicators, ohlcv_df):
    strategy_name = f"strategy_{strategy_num}"

    if not selected_indicators:
        return None, None, None, None, None, None

    sig_columns = ", ".join([f"sig_{ind}" for ind in selected_indicators])
    query = text(
        f"SELECT datetime, {sig_columns} FROM signals.bybit_btc_1h_signals ORDER BY datetime"
    )

    with get_db_connection(engine) as conn:
        signal_df = pd.read_sql_query(query, conn)

    signal_df = ensure_utc_datetime(signal_df)
    ohlcv_df = ensure_utc_datetime(ohlcv_df)

    signal_df["signal"] = np.sign(
        signal_df[[f"sig_{ind}" for ind in selected_indicators]].sum(axis=1)
    )

    signal_df[["datetime", "signal"]].to_sql(
        strategy_name,
        engine,
        schema="strategy_signal",
        if_exists="replace",
        index=False,
        method="multi",
    )

    merged_df = pd.merge(
        ohlcv_df[["datetime", "open", "high", "low"]],
        signal_df[["datetime", "signal"]],
        on="datetime",
        how="left",
    )
    merged_df["signal"] = merged_df["signal"].fillna(0).astype(int)

    backtest_results = backtest_strategy(
        merged_df, tp=TP, sl=SL, fee_rate=FEE_RATE, initial_balance=INITIAL_BALANCE
    )

    metrics = calculate_performance_metrics(backtest_results)

    if metrics["pnl_sum"] < MIN_PNL_SUM:
        print(
            f"Strategy {strategy_num} filtered out (PnL Sum: {metrics['pnl_sum']:.2f} < {MIN_PNL_SUM})"
        )
        return None, None, None, None, None, None

    backtest_results.to_sql(
        f"backtest_{strategy_num}",
        engine,
        schema="strategy_backtest",
        if_exists="replace",
        index=False,
        method="multi",
    )

    with get_db_connection(engine) as conn:
        # Create indicator mapping for update (TRUE for selected indicators, FALSE for others)
        all_indicators = get_available_indicators(engine)
        indicator_values = {ind: ind in selected_indicators for ind in all_indicators}

        conn.execute(
            text(
                f"""
                INSERT INTO public.strategies 
                (strategy_name, exchange, symbol, final_balance, pnl_sum, 
                 sharpe_ratio, total_trades, win_rate, max_drawdown, {', '.join(all_indicators)})
                VALUES (:name, :exchange, :symbol, :final_balance, :pnl_sum, 
                        :sharpe_ratio, :total_trades, :win_rate, :max_drawdown, {', '.join([f':{ind}' for ind in all_indicators])})
            """
            ),
            {
                "name": strategy_name,
                "exchange": EXCHANGE,
                "symbol": SYMBOL,
                "final_balance": metrics["final_balance"],
                "pnl_sum": metrics["pnl_sum"],
                "sharpe_ratio": metrics["sharpe_ratio"],
                "total_trades": metrics["total_trades"],
                "win_rate": metrics["win_rate"],
                "max_drawdown": metrics["max_drawdown"],
                **indicator_values,
            },
        )
        conn.commit()

    return (
        strategy_name,
        metrics["final_balance"],
        metrics["pnl_sum"],
        metrics["sharpe_ratio"],
        metrics["total_trades"],
        metrics["win_rate"],
    )


def optimize_strategy(engine, all_indicators, ohlcv_df, trial_number):
    sampler = TPESampler(seed=42 + trial_number)  # Different seed for each trial
    study = optuna.create_study(direction="maximize", sampler=sampler)
    tested_combinations = set()

    def objective(trial):
        selected_indicators = [
            ind
            for ind in all_indicators
            if trial.suggest_categorical(f"use_{ind}", [True, False])
        ]

        combo_key = tuple(sorted(selected_indicators))
        if not selected_indicators or combo_key in tested_combinations:
            return 0.0

        tested_combinations.add(combo_key)

        sig_columns = ", ".join([f"sig_{ind}" for ind in selected_indicators])
        query = text(
            f"SELECT datetime, {sig_columns} FROM signals.bybit_btc_1h_signals ORDER BY datetime"
        )

        with get_db_connection(engine) as conn:
            signal_df = pd.read_sql_query(query, conn)

        signal_df = ensure_utc_datetime(signal_df)
        ohlcv_df_copy = ensure_utc_datetime(ohlcv_df.copy())

        signal_df["signal"] = np.sign(
            signal_df[[f"sig_{ind}" for ind in selected_indicators]].sum(axis=1)
        )

        merged_df = pd.merge(
            ohlcv_df_copy[["datetime", "open", "high", "low"]],
            signal_df[["datetime", "signal"]],
            on="datetime",
            how="left",
        )
        merged_df["signal"] = merged_df["signal"].fillna(0).astype(int)

        results = backtest_strategy(
            merged_df, tp=TP, sl=SL, fee_rate=FEE_RATE, initial_balance=INITIAL_BALANCE
        )

        if results.empty or "pnl_sum" not in results.columns:
            return 0.0

        pnl_sum = results["pnl_sum"].iloc[-1]
        if pnl_sum < MIN_PNL_SUM:
            return 0.0

        trial.set_user_attr("selected_indicators", selected_indicators)
        trial.set_user_attr("pnl_sum", pnl_sum)
        return pnl_sum

    study.optimize(objective, n_trials=1)  # One trial per study to ensure uniqueness

    valid_trials = [
        t for t in study.trials if t.user_attrs.get("pnl_sum", 0) >= MIN_PNL_SUM
    ]

    if not valid_trials:
        return []

    return [valid_trials[0].user_attrs["selected_indicators"]]


def main():
    engine = get_db_engine()

    try:
        with get_db_connection(engine) as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS signals"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS strategy_signal"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS strategy_backtest"))
            conn.commit()

        ohlcv_df = pd.read_sql_query(
            text(
                "SELECT datetime, open, high, low FROM bybit_data.btc_usdt_1m ORDER BY datetime"
            ),
            engine,
        )
        ohlcv_df = ensure_utc_datetime(ohlcv_df)

        all_indicators = get_available_indicators(engine)
        print(f"Found {len(all_indicators)} indicators: {all_indicators}")
        create_strategy_table(engine, all_indicators)

        successful_strategies = 0
        for i in range(1, 6):  # Generate exactly 5 strategies
            print(f"\nCreating strategy {i}/5")

            try:
                best_indicators = optimize_strategy(engine, all_indicators, ohlcv_df, i)
                if not best_indicators:
                    print("No valid strategies found in this optimization run")
                    continue

                print(f"Selected indicators: {best_indicators[0]}")

                strategy_name, final_balance, pnl_sum, sharpe, trades, win_rate = (
                    generate_strategy(engine, i, best_indicators[0], ohlcv_df)
                )

                if strategy_name is None:
                    continue

                successful_strategies += 1
                print(f"Created {strategy_name}")
                print(f"Final Balance: {final_balance:.2f}")
                print(f"PnL Sum: {pnl_sum:.2f}")
                print(f"Sharpe Ratio: {sharpe:.2f}")
                print(f"Total Trades: {trades}")
                print(f"Win Rate: {win_rate:.2%}")

            except Exception as e:
                print(f"Failed to create strategy {i}: {str(e)}")
                continue

    except Exception as e:
        print(f"\nFatal error: {str(e)}")
    finally:
        engine.dispose()

    print(f"\nCreated {successful_strategies} valid strategies.")


if __name__ == "__main__":
    main()
