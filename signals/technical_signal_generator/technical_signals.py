import numpy as np
import pandas as pd

PRICE_COLS = ['datetime', 'open', 'high', 'low', 'close', 'volume']

# ------------------------------------------------------------------------
def getSignals(df: pd.DataFrame, indicators) -> pd.DataFrame:
    """
    Parameters
    ----------
    df          : DataFrame already containing raw indicator columns.
    indicators  : str | list | set
        e.g.  "sma,ema,bb,rsi,macd,stoch"  or  ["sma","ema","bb",...]
        Case‑insensitive.  Signals created only for these names.

    Returns
    -------
    DataFrame with PRICE_COLS + sig_* columns (no duplicates)
    """

    # normalise user input ------------------------------------------------
    if isinstance(indicators, str):
        indicators = [i.strip() for i in indicators.split(',') if i.strip()]
    indset = {i.lower() for i in indicators}          # case‑insensitive

    # --------------------------------------------------------------------
    # 1.  SELECTIVE SIGNAL GENERATION
    # --------------------------------------------------------------------

    # Moving‑average slope -----------------------------------------------
    ma_cols = ['sma', 'ema', 'wma', 'tema', 'trima', 'kama', 'dema', 't3',
               'ht_trendline']
    for col in ma_cols:
        if col in indset and col in df.columns:
            df[f'sig_{col}'] = (
                np.sign(df[col].diff())
                  .replace(0, np.nan)
                  .ffill()
                  .fillna(0)
            )

    # Price‑vs‑line comparisons (SAR / Mid‑* / MAMA) ---------------------
    for col in ['sar', 'sar_ext', 'midpoint', 'midprice']:
        if col in indset and col in df.columns:
            df[f'sig_{col}'] = np.sign(df['close'] - df[col]).replace(0, 0)

    if {'mama', 'fama'}.issubset(df.columns) and 'mama' in indset:
        df['sig_mama'] = np.sign(df['mama'] - df['fama']).replace(0, 0)

    # RSI -----------------------------------------------------------------
    if 'rsi' in indset and 'rsi' in df.columns:
        df['sig_rsi'] = np.select(
            [df['rsi'] > 55, df['rsi'] < 45], [1, -1], default=0
        )

    # MACD histogram ------------------------------------------------------
    if 'macd' in indset and 'macd_hist' in df.columns:
        df['sig_macd'] = np.sign(df['macd_hist']).replace(0, 0)

    # Stoch variants ------------------------------------------------------
    if 'stoch' in indset and {'stoch_k', 'stoch_d'}.issubset(df.columns):
        df['sig_stoch'] = np.select(
            [(df['stoch_k'] > df['stoch_d']) & (df['stoch_k'] > 50),
             (df['stoch_k'] < df['stoch_d']) & (df['stoch_k'] < 50)],
            [1, -1], default=0
        )
    if 'stochf' in indset and {'stochf_k', 'stochf_d'}.issubset(df.columns):
        df['sig_stochf'] = np.sign(df['stochf_k'] - df['stochf_d']).replace(0, 0)
    if 'stochrsi' in indset and {'stochrsi_k', 'stochrsi_d'}.issubset(df.columns):
        df['sig_stochrsi'] = np.sign(df['stochrsi_k'] - df['stochrsi_d']).replace(0, 0)

    # CCI, ROC, MOM -------------------------------------------------------
    if 'cci' in indset and 'cci' in df.columns:
        df['sig_cci'] = np.select(
            [df['cci'] > 100, df['cci'] < -100], [1, -1], default=0
        )
    if 'roc' in indset and 'roc' in df.columns:
        df['sig_roc'] = np.sign(df['roc']).replace(0, 0)
    if 'mom' in indset and 'mom' in df.columns:
        df['sig_mom'] = np.sign(df['mom']).replace(0, 0)

    # MFI -----------------------------------------------------------------
    if 'mfi' in indset and 'mfi' in df.columns:
        df['sig_mfi'] = np.select(
            [df['mfi'] > 60, df['mfi'] < 40], [1, -1], default=0
        )

    # ADX / ADXR ----------------------------------------------------------
    if 'adx' in indset and 'adx' in df.columns:
        trend_dir = np.sign(df['close'].diff())
        df['sig_adx'] = np.where(df['adx'] > 25, trend_dir, 0)
    if 'adxr' in indset and 'adxr' in df.columns:
        df['sig_adxr'] = np.where(df['adxr'] > 25, 1, 0)

    # PLUS_DI vs MINUS_DI -------------------------------------------------
    if 'di' in indset and {'plus_di', 'minus_di'}.issubset(df.columns):
        df['sig_di'] = np.select(
            [df['plus_di'] > df['minus_di'],
             df['plus_di'] < df['minus_di']],
            [1, -1], default=0
        )

    # Aroon ---------------------------------------------------------------
    if 'aroon' in indset and {'aroon_up', 'aroon_down'}.issubset(df.columns):
        df['sig_aroon'] = np.select(
            [df['aroon_up'] > df['aroon_down'],
             df['aroon_up'] < df['aroon_down']],
            [1, -1], default=0
        )
    if 'aroonosc' in indset and 'aroonosc' in df.columns:
        df['sig_aroonosc'] = np.sign(df['aroonosc']).replace(0, 0)

    # PPO, APO, CMO, BOP, DX, PPO/ROCR etc. -------------------------------
    simple_sign = ['apo', 'bop', 'cmo', 'dx', 'ppo',
                   'rocp', 'rocr', 'rocr100',
                   'trix', 'ultosc', 'willr']
    for name in simple_sign:
        if name in indset and name in df.columns:
            df[f'sig_{name}'] = np.sign(df[name]).replace(0, 0)

    # Bollinger Bands -----------------------------------------------------
    if 'bb' in indset and {'close','bb_upper','bb_lower'}.issubset(df.columns):
        df['sig_bbands'] = np.select(
            [df['close'] > df['bb_upper'],
             df['close'] < df['bb_lower']],
            [1, -1], default=0
        )

    # OBV / ADOSC ---------------------------------------------------------
    if 'obv' in indset and 'obv' in df.columns:
        df['sig_obv'] = np.sign(df['obv'].diff()).replace(0, 0)
    if 'adosc' in indset and 'adosc' in df.columns:
        df['sig_adosc'] = np.sign(df['adosc']).replace(0, 0)

    # ATR – neutral by default -------------------------------------------
    if 'atr' in indset and 'atr' in df.columns:
        df['sig_atr'] = 0

    # Candlestick patterns -----------------------------------------------
    if any(col.startswith('cdl') for col in df.columns):
        for col in df.columns:
            if col.startswith('cdl') and col.replace('cdl','') in indset:
                df[f'sig_{col}'] = np.sign(df[col])

    # --------------------------------------------------------------------
    # 2.  KEEP PRICE + sig_* ONLY
    # --------------------------------------------------------------------
    keep_cols = PRICE_COLS + [c for c in df.columns if c.startswith('sig_')]
    df_out = df.loc[:, keep_cols]

    return df_out.reset_index(drop=True)
