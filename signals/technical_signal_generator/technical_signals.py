import numpy as np
import pandas as pd

PRICE_COLS = ["datetime", "open", "high", "low", "close", "volume"]


# ------------------------------------------------------------------------
def getSignals(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.sort_values("datetime", inplace=True)
    df.reset_index(drop=True, inplace=True)

    PRICE_COLS = ["datetime", "open", "high", "low", "close", "volume"]

    # --- MA slopes
    ma_cols = [
        "sma",
        "ema",
        "wma",
        "tema",
        "trima",
        "kama",
        "dema",
        "t3",
        "ht_trendline",
    ]
    for col in ma_cols:
        if col in df.columns:
            slope = df[col].diff()
            df[f"sig_{col}"] = np.sign(slope).fillna(0)

    # --- Price vs. line comparisons
    for col in ["sar", "sar_ext", "midpoint", "midprice"]:
        if col in df.columns:
            df[f"sig_{col}"] = np.sign(df["close"] - df[col]).fillna(0)

    if {"mama", "fama"}.issubset(df.columns):
        df["sig_mama"] = np.sign(df["mama"] - df["fama"]).fillna(0)

    # --- RSI
    if "rsi" in df.columns:
        df["sig_rsi"] = np.select([df["rsi"] > 55, df["rsi"] < 45], [1, -1], default=0)

    # --- MACD
    if "macd_hist" in df.columns:
        df["sig_macd"] = np.sign(df["macd_hist"]).fillna(0)

    # --- Stochastic
    if {"stoch_k", "stoch_d"}.issubset(df.columns):
        df["sig_stoch"] = np.select(
            [
                (df["stoch_k"] > df["stoch_d"]) & (df["stoch_k"] > 50),
                (df["stoch_k"] < df["stoch_d"]) & (df["stoch_k"] < 50),
            ],
            [1, -1],
            default=0,
        )
    if {"stochf_k", "stochf_d"}.issubset(df.columns):
        df["sig_stochf"] = np.sign(df["stochf_k"] - df["stochf_d"]).fillna(0)
    if {"stochrsi_k", "stochrsi_d"}.issubset(df.columns):
        df["sig_stochrsi"] = np.sign(df["stochrsi_k"] - df["stochrsi_d"]).fillna(0)

    # --- CCI, ROC, MOM
    if "cci" in df.columns:
        df["sig_cci"] = np.select(
            [df["cci"] > 100, df["cci"] < -100], [1, -1], default=0
        )
    if "roc" in df.columns:
        df["sig_roc"] = np.sign(df["roc"]).fillna(0)
    if "mom" in df.columns:
        df["sig_mom"] = np.sign(df["mom"]).fillna(0)

    # --- MFI
    if "mfi" in df.columns:
        df["sig_mfi"] = np.select([df["mfi"] > 60, df["mfi"] < 40], [1, -1], default=0)

    # --- ADX / ADXR
    if "adx" in df.columns:
        trend_dir = np.sign(df["close"].diff()).fillna(0)
        df["sig_adx"] = np.where(df["adx"] > 25, trend_dir, 0)
    if "adxr" in df.columns:
        df["sig_adxr"] = np.where(df["adxr"] > 25, 1, 0)

    # --- DI
    if {"plus_di", "minus_di"}.issubset(df.columns):
        df["sig_di"] = np.select(
            [df["plus_di"] > df["minus_di"], df["plus_di"] < df["minus_di"]],
            [1, -1],
            default=0,
        )

    # --- Aroon
    if {"aroon_up", "aroon_down"}.issubset(df.columns):
        df["sig_aroon"] = np.select(
            [df["aroon_up"] > df["aroon_down"], df["aroon_up"] < df["aroon_down"]],
            [1, -1],
            default=0,
        )
    if "aroonosc" in df.columns:
        df["sig_aroonosc"] = np.sign(df["aroonosc"]).fillna(0)

    # --- Sign indicators
    for name in [
        "apo",
        "bop",
        "cmo",
        "dx",
        "ppo",
        "rocp",
        "rocr",
        "rocr100",
        "trix",
        "ultosc",
        "willr",
    ]:
        if name in df.columns:
            df[f"sig_{name}"] = np.sign(df[name]).fillna(0)

    # --- Bollinger Bands
    if {"close", "bb_upper", "bb_lower"}.issubset(df.columns):
        df["sig_bbands"] = np.select(
            [df["close"] > df["bb_upper"], df["close"] < df["bb_lower"]],
            [1, -1],
            default=0,
        )

    # --- OBV / ADOSC
    if "obv" in df.columns:
        df["sig_obv"] = np.sign(df["obv"].diff()).fillna(0)
    if "adosc" in df.columns:
        df["sig_adosc"] = np.sign(df["adosc"]).fillna(0)

    # --- ATR – neutral
    if "atr" in df.columns:
        df["sig_atr"] = 0

    # --- Candlestick patterns
    for col in df.columns:
        if col.startswith("cdl"):
            df[f"sig_{col}"] = np.sign(df[col]).fillna(0)

    # --- Final cleanup
    sig_cols = [col for col in df.columns if col.startswith("sig_")]
    df[sig_cols] = df[sig_cols].fillna(0)

    keep_cols = [col for col in PRICE_COLS if col in df.columns] + sig_cols
    return df[keep_cols].reset_index(drop=True)


def get_last_signal_timestamp(engine, signal_table_full):
    query = f"""
        SELECT MAX(datetime) AS last_dt FROM {signal_table_full}
    """
    df = pd.read_sql(query, engine)
    if not df.empty and df.iloc[0]["last_dt"] is not None:
        return pd.to_datetime(df.iloc[0]["last_dt"]).tz_localize(None)
    return None
