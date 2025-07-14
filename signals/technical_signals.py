import pandas as pd
import numpy as np
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), 'data'))

def getSignals(df):
    ma_cols = ['sma', 'ema', 'wma', 'tema',
               'trima', 'kama', 'dema']
    for col in ma_cols:
        df[f'sig_{col}'] = np.sign(df[col].diff()).replace(0, np.nan).fillna(method='ffill').fillna(0)

    # --- RSI ---------------------------------------------------------------
    df['sig_rsi'] = np.select(
        [(df['rsi'] > 55),   (df['rsi'] < 45)],
        [1,                     -1],
        default=0
    )

    # --- MACD Histogram ----------------------------------------------------
    df['sig_macd'] = np.sign(df['macd_hist']).replace(0, 0)

    # --- Stochastic: %K vs %D ---------------------------------------------
    df['sig_stoch'] = np.select(
        [(df['stoch_k'] > df['stoch_d']) & (df['stoch_k'] > 50),
         (df['stoch_k'] < df['stoch_d']) & (df['stoch_k'] < 50)],
        [1, -1], default=0
    )

    # --- CCI ---------------------------------------------------------------
    df['sig_cci'] = np.select(
        [df['cci'] > 100, df['cci'] < -100],
        [1, -1], default=0)

    # --- ROC & MOM ---------------------------------------------------------
    df['sig_roc'] = np.sign(df['roc']).replace(0, 0)
    df['sig_mom'] = np.sign(df['mom']).replace(0, 0)

    # --- MFI ---------------------------------------------------------------
    df['sig_mfi'] = np.select(
        [df['mfi'] > 60, df['mfi'] < 40],
        [1, -1], default=0)

    # --- ADX (directionless strength) -------------------------------------
    # Here: only flag strong trend if ADX>25, then follow close-to-close move
    trend_dir = np.sign(df['close'].diff())
    df['sig_adx'] = np.where(df['adx'] > 25, trend_dir, 0)

    # --- Bollinger Bands ---------------------------------------------------
    df['sig_bbands'] = np.select(
        [df['close'] > df['bb_upper'],
         df['close'] < df['bb_lower']],
        [1, -1], default=0)

    # --- OBV slope ---------------------------------------------------------
    df['sig_obv'] = np.sign(df['obv'].diff()).replace(0, 0)

    # --- ADOSC sign --------------------------------------------------------
    df['sig_adosc'] = np.sign(df['adosc']).replace(0, 0)

    # --- ATR is volatility, not direction → default 0 ----------------------
    df['sig_atr'] = 0

    new_df = df.drop(columns=['bb_upper', 'bb_middle', 'bb_lower', 'dema', 'sma',
                     'ema', 'wma', 'tema', 'trima', 'kama', 'cci', 'roc',
                     'mom', 'mfi', 'adx', 'atr', 'obv', 'adosc', 'rsi',
                     'macd', 'macd_sig', 'macd_hist', 'stoch_k', 'stoch_d'])

    return new_df.reset_index(drop=True)