import numpy as np
import pandas as pd
import os
import sys
import talib

sys.path.append(os.path.join(os.path.dirname(__file__), 'data'))

def _price_arrays(df):
    return (df['open'].values,
            df['high'].values,
            df['low'].values,
            df['close'].values,
            df['volume'].values)

def generateIndicators(df: pd.DataFrame) -> pd.DataFrame:
    tp = 20
    df = pd.read_csv("def.csv")
    df = df.copy().reset_index(drop=True)
    df['dema'] = talib.DEMA(df['close'], timeperiod=20)
    df['sma'] = talib.SMA(df['close'])
    df['ema'] = talib.EMA(df['close'],   timeperiod=20)          # Exponential MA
    df['wma'] = talib.WMA(df['close'],   timeperiod=20)          # Weighted MA
    df['tema'] = talib.TEMA(df['close'],  timeperiod=20)          # Triple‑EMA
    df['trima'] = talib.TRIMA(df['close'], timeperiod=20)          # Triangular MA
    df['kama'] = talib.KAMA(df['close'],  timeperiod=20)          # Kaufman MA
    df['cci'] = talib.CCI(df['high'], df['low'], df['close'], timeperiod=20) # Commodity Channel
    df['roc'] = talib.ROC(df['close'], timeperiod=20)                        # Rate of Change
    df['mom'] = talib.MOM(df['close'], timeperiod=20)                        # Momentum
    df['mfi'] = talib.MFI(df['high'], df['low'], df['close'], df['volume'], timeperiod=20)  # Money‑Flow
    df['adx'] = talib.ADX(df['high'], df['low'], df['close'], timeperiod=20)  
    df['atr'] = talib.ATR(df['high'], df['low'], df['close'], timeperiod=20)     # Average True Range
    obv = (talib.OBV(df['close'], df['volume']))
    df['obv'] = obv.tail(20)                           
    df['adosc'] = talib.ADOSC(df['high'], df['low'], df['close'],df['volume'], fastperiod=3, slowperiod=10)  

    df['rsi'] = talib.RSI(df['close'], timeperiod=20)                 # Relative Strength
    macd, macd_sig, macd_hist = talib.MACD(df['close'], fastperiod=12,
                                   slowperiod=26, signalperiod=9)
    df['macd']      = macd
    df['macd_sig']  = macd_sig
    df['macd_hist'] = macd_hist

    slowk, slowd    = talib.STOCH(df['high'], df['low'], df['close'],
                          fastk_period=14, slowk_period=3, slowd_period=3)
    df['stoch_k']   = slowk
    df['stoch_d']   = slowd

    
    o, h, l, c, v = _price_arrays(df)

    # ----- Trend / MA family (already have SMA/EMA/WMA/TEMA/DEMA/TRIMA/KAMA) -----
    df['bb_upper'], df['bb_mid'], df['bb_lower'] = talib.BBANDS(c, timeperiod=tp)
    df['ht_trendline'] = talib.HT_TRENDLINE(c)
    df['mama'], df['fama'] = talib.MAMA(c)                     # adaptive MA
    df['sar']   = talib.SAR(h, l)                              # Parabolic SAR
    df['sar_ext'] = talib.SAREXT(h, l)                         # Extended SAR
    df['midpoint'] = talib.MIDPOINT(c, timeperiod=tp)
    df['midprice'] = talib.MIDPRICE(h, l, timeperiod=tp)
    df['t3']    = talib.T3(c, timeperiod=tp)

    # ----- Momentum / Oscillators -----------------------------------------------
    df['adxr']  = talib.ADXR(h, l, c, timeperiod=14)
    df['apo']   = talib.APO(c, fastperiod=12, slowperiod=26)   # Absolute Price Osc
    aroond, aroonu = talib.AROON(h, l, timeperiod=tp)
    df['aroon_down'], df['aroon_up'] = aroond, aroonu
    df['aroonosc'] = talib.AROONOSC(h, l, timeperiod=tp)
    df['bop']   = talib.BOP(o, h, l, c)
    df['cmo']   = talib.CMO(c, timeperiod=tp)
    df['dx']    = talib.DX(h, l, c, timeperiod=14)
    macdext, macdsigext, macdhistext = talib.MACDEXT(c)
    df['macdext'], df['macdsigext'], df['macdhistext'] = macdext, macdsigext, macdhistext
    macdfix, macdsigfix, macdhistfix = talib.MACDFIX(c, signalperiod=9)
    df['macdfix'], df['macdsigfix'], df['macdhistfix'] = macdfix, macdsigfix, macdhistfix
    df['minus_di'] = talib.MINUS_DI(h, l, c, timeperiod=14)
    df['minus_dm'] = talib.MINUS_DM(h, l, timeperiod=14)
    df['plus_di']  = talib.PLUS_DI(h, l, c, timeperiod=14)
    df['plus_dm']  = talib.PLUS_DM(h, l, timeperiod=14)
    df['ppo']      = talib.PPO(c)
    df['rocp']     = talib.ROCP(c, timeperiod=tp)
    df['rocr']     = talib.ROCR(c, timeperiod=tp)
    df['rocr100']  = talib.ROCR100(c, timeperiod=tp)
    fastk, fastd   = talib.STOCHF(h, l, c)
    df['stochf_k'], df['stochf_d'] = fastk, fastd
    sr_k, sr_d     = talib.STOCHRSI(c)
    df['stochrsi_k'], df['stochrsi_d'] = sr_k, sr_d
    df['trix']     = talib.TRIX(c, timeperiod=30)
    df['ultosc']   = talib.ULTOSC(h, l, c)
    df['willr']    = talib.WILLR(h, l, c, timeperiod=14)

    # ----- Volume ----------------------------------------------------------------
    df['ad']    = talib.AD(h, l, c, v)
    df['adosc'] = talib.ADOSC(h, l, c, v)      # OBV already exists

    # ----- Hilbert Transform set --------------------------------------------------
    df['ht_dcperiod'] = talib.HT_DCPERIOD(c)
    df['ht_dcphase']  = talib.HT_DCPHASE(c)
    inphase, quad = talib.HT_PHASOR(c)
    df['ht_phasor_in'], df['ht_phasor_quad'] = inphase, quad
    sine, leadsine = talib.HT_SINE(c)
    df['ht_sine'], df['ht_leadsine'] = sine, leadsine
    df['ht_trendmode'] = talib.HT_TRENDMODE(c)

    # ----- Price transforms -------------------------------------------------------
    df['avgprice'] = talib.AVGPRICE(o, h, l, c)
    df['medprice'] = talib.MEDPRICE(h, l)
    df['typprice'] = talib.TYPPRICE(h, l, c)
    df['wclprice'] = talib.WCLPRICE(h, l, c)

    # ----- Volatility -------------------------------------------------------------
    df['atr']   = talib.ATR(h, l, c, timeperiod=14)
    df['natr']  = talib.NATR(h, l, c, timeperiod=14)
    df['trange'] = talib.TRANGE(h, l, c)

    # ----- Statistical / Regression ----------------------------------------------
    df['beta']  = talib.BETA(h, l, timeperiod=tp)
    df['correl'] = talib.CORREL(h, l, timeperiod=tp)
    df['linearreg']  = talib.LINEARREG(c, timeperiod=tp)
    df['linearreg_angle'] = talib.LINEARREG_ANGLE(c, timeperiod=tp)
    df['linearreg_intercept'] = talib.LINEARREG_INTERCEPT(c, timeperiod=tp)
    df['linearreg_slope'] = talib.LINEARREG_SLOPE(c, timeperiod=tp)
    df['stddev'] = talib.STDDEV(c, timeperiod=tp)
    df['tsf']    = talib.TSF(c, timeperiod=tp)
    df['var']    = talib.VAR(c, timeperiod=tp)

    # ----- Candlestick patterns ---------------------------------------------------
    for fn in talib.get_function_groups()['Pattern Recognition']:
        col = fn.lower()
        df[col] = getattr(talib, fn)(o, h, l, c)   # returns ±100 / 0

    df = df.iloc[20:]
    
    
    
    return df.reset_index(drop=True)
        