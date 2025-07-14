import numpy as np
import pandas as pd
import os
import sys
import talib

sys.path.append(os.path.join(os.path.dirname(__file__), 'data'))

def generateIndicators():
    df = pd.read_csv("def.csv")
    upper, middle, lower = talib.BBANDS(
        df['close'],
        timeperiod=20,   # look‑back window
        nbdevup=2,       # σ to add to SMA for upper band
        nbdevdn=2,       # σ to subtract from SMA for lower band
        matype=0         # 0 = SMA; 1 = EMA etc.
    )
    new_df = pd.DataFrame()
    
    new_df = df.copy().reset_index(drop=True)
    new_df['bb_upper'] = upper
    new_df['bb_middle'] = middle
    new_df['bb_lower'] = lower
    new_df['dema'] = talib.DEMA(df['close'], timeperiod=20)
    new_df['sma'] = talib.SMA(df['close'])
    new_df['ema'] = talib.EMA(df['close'],   timeperiod=20)          # Exponential MA
    new_df['wma'] = talib.WMA(df['close'],   timeperiod=20)          # Weighted MA
    new_df['tema'] = talib.TEMA(df['close'],  timeperiod=20)          # Triple‑EMA
    new_df['trima'] = talib.TRIMA(df['close'], timeperiod=20)          # Triangular MA
    new_df['kama'] = talib.KAMA(df['close'],  timeperiod=20)          # Kaufman MA
    new_df['cci'] = talib.CCI(df['high'], df['low'], df['close'], timeperiod=20) # Commodity Channel
    new_df['roc'] = talib.ROC(df['close'], timeperiod=20)                        # Rate of Change
    new_df['mom'] = talib.MOM(df['close'], timeperiod=20)                        # Momentum
    new_df['mfi'] = talib.MFI(df['high'], df['low'], df['close'], df['volume'], timeperiod=20)  # Money‑Flow
    new_df['adx'] = talib.ADX(df['high'], df['low'], df['close'], timeperiod=20)  
    new_df['atr'] = talib.ATR(df['high'], df['low'], df['close'], timeperiod=20)     # Average True Range
    obv = (talib.OBV(df['close'], df['volume']))
    new_df['obv'] = obv.tail(20)                           
    new_df['adosc'] = talib.ADOSC(df['high'], df['low'], df['close'],df['volume'], fastperiod=3, slowperiod=10)  

    new_df['rsi'] = talib.RSI(df['close'], timeperiod=20)                 # Relative Strength
    macd, macd_sig, macd_hist = talib.MACD(df['close'], fastperiod=12,
                                   slowperiod=26, signalperiod=9)
    new_df['macd']      = macd
    new_df['macd_sig']  = macd_sig
    new_df['macd_hist'] = macd_hist

    slowk, slowd    = talib.STOCH(df['high'], df['low'], df['close'],
                          fastk_period=14, slowk_period=3, slowd_period=3)
    new_df['stoch_k']   = slowk
    new_df['stoch_d']   = slowd

    new_df = new_df.iloc[20:]
    
    return new_df.reset_index(drop=True)
        