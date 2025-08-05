import configparser
from datetime import datetime
import os
import sys
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import talib

# Add subfolder to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


def _price_arrays(df):
    return (
        df["open"].values,
        df["high"].values,
        df["low"].values,
        df["close"].values,
        df["volume"].values,
    )


def get_last_indicator_timestamp(engine, schema, table):
    query = f"SELECT MAX(datetime) FROM {schema}.{table}"
    try:
        with engine.connect() as conn:
            result = conn.execute(query).scalar()
        return pd.to_datetime(result) if result else None
    except Exception as e:
        print(f"⚠️ Could not get last indicator timestamp: {e}")
        return None


def generateIndicators(df: pd.DataFrame) -> pd.DataFrame:
    tp = 20
    df["dema"] = talib.DEMA(df["close"], timeperiod=20)
    df["sma"] = talib.SMA(df["close"])
    df["ema"] = talib.EMA(df["close"], timeperiod=20)  # Exponential MA
    df["wma"] = talib.WMA(df["close"], timeperiod=20)  # Weighted MA
    df["tema"] = talib.TEMA(df["close"], timeperiod=20)  # Triple‑EMA
    df["trima"] = talib.TRIMA(df["close"], timeperiod=20)  # Triangular MA
    df["kama"] = talib.KAMA(df["close"], timeperiod=20)  # Kaufman MA
    df["cci"] = talib.CCI(
        df["high"], df["low"], df["close"], timeperiod=20
    )  # Commodity Channel
    df["roc"] = talib.ROC(df["close"], timeperiod=20)  # Rate of Change
    df["mom"] = talib.MOM(df["close"], timeperiod=20)  # Momentum
    df["mfi"] = talib.MFI(
        df["high"], df["low"], df["close"], df["volume"], timeperiod=20
    )  # Money‑Flow
    df["adx"] = talib.ADX(df["high"], df["low"], df["close"], timeperiod=20)
    df["atr"] = talib.ATR(
        df["high"], df["low"], df["close"], timeperiod=20
    )  # Average True Range
    obv = talib.OBV(df["close"], df["volume"])
    df["obv"] = obv.tail(20)
    df["adosc"] = talib.ADOSC(
        df["high"], df["low"], df["close"], df["volume"], fastperiod=3, slowperiod=10
    )

    df["rsi"] = talib.RSI(df["close"], timeperiod=20)  # Relative Strength
    macd, macd_sig, macd_hist = talib.MACD(
        df["close"], fastperiod=12, slowperiod=26, signalperiod=9
    )
    df["macd"] = macd
    df["macd_sig"] = macd_sig
    df["macd_hist"] = macd_hist

    slowk, slowd = talib.STOCH(
        df["high"],
        df["low"],
        df["close"],
        fastk_period=14,
        slowk_period=3,
        slowd_period=3,
    )
    df["stoch_k"] = slowk
    df["stoch_d"] = slowd

    o, h, l, c, v = _price_arrays(df)

    # ----- Trend / MA family (already have SMA/EMA/WMA/TEMA/DEMA/TRIMA/KAMA) -----
    df["bb_upper"], df["bb_mid"], df["bb_lower"] = talib.BBANDS(c, timeperiod=tp)
    df["ht_trendline"] = talib.HT_TRENDLINE(c)
    df["mama"], df["fama"] = talib.MAMA(c)  # adaptive MA
    df["sar"] = talib.SAR(h, l)  # Parabolic SAR
    df["sar_ext"] = talib.SAREXT(h, l)  # Extended SAR
    df["midpoint"] = talib.MIDPOINT(c, timeperiod=tp)
    df["midprice"] = talib.MIDPRICE(h, l, timeperiod=tp)
    df["t3"] = talib.T3(c, timeperiod=tp)

    # ----- Momentum / Oscillators -----------------------------------------------
    df["adxr"] = talib.ADXR(h, l, c, timeperiod=14)
    df["apo"] = talib.APO(c, fastperiod=12, slowperiod=26)  # Absolute Price Osc
    aroond, aroonu = talib.AROON(h, l, timeperiod=tp)
    df["aroon_down"], df["aroon_up"] = aroond, aroonu
    df["aroonosc"] = talib.AROONOSC(h, l, timeperiod=tp)
    df["bop"] = talib.BOP(o, h, l, c)
    df["cmo"] = talib.CMO(c, timeperiod=tp)
    df["dx"] = talib.DX(h, l, c, timeperiod=14)
    macdext, macdsigext, macdhistext = talib.MACDEXT(c)
    df["macdext"], df["macdsigext"], df["macdhistext"] = (
        macdext,
        macdsigext,
        macdhistext,
    )
    macdfix, macdsigfix, macdhistfix = talib.MACDFIX(c, signalperiod=9)
    df["macdfix"], df["macdsigfix"], df["macdhistfix"] = (
        macdfix,
        macdsigfix,
        macdhistfix,
    )
    df["minus_di"] = talib.MINUS_DI(h, l, c, timeperiod=14)
    df["minus_dm"] = talib.MINUS_DM(h, l, timeperiod=14)
    df["plus_di"] = talib.PLUS_DI(h, l, c, timeperiod=14)
    df["plus_dm"] = talib.PLUS_DM(h, l, timeperiod=14)
    df["ppo"] = talib.PPO(c)
    df["rocp"] = talib.ROCP(c, timeperiod=tp)
    df["rocr"] = talib.ROCR(c, timeperiod=tp)
    df["rocr100"] = talib.ROCR100(c, timeperiod=tp)
    fastk, fastd = talib.STOCHF(h, l, c)
    df["stochf_k"], df["stochf_d"] = fastk, fastd
    sr_k, sr_d = talib.STOCHRSI(c)
    df["stochrsi_k"], df["stochrsi_d"] = sr_k, sr_d
    df["trix"] = talib.TRIX(c, timeperiod=30)
    df["ultosc"] = talib.ULTOSC(h, l, c)
    df["willr"] = talib.WILLR(h, l, c, timeperiod=14)

    # ----- Volume ----------------------------------------------------------------
    df["ad"] = talib.AD(h, l, c, v)
    df["adosc"] = talib.ADOSC(h, l, c, v)  # OBV already exists

    # ----- Hilbert Transform set --------------------------------------------------
    df["ht_dcperiod"] = talib.HT_DCPERIOD(c)
    df["ht_dcphase"] = talib.HT_DCPHASE(c)
    inphase, quad = talib.HT_PHASOR(c)
    df["ht_phasor_in"], df["ht_phasor_quad"] = inphase, quad
    sine, leadsine = talib.HT_SINE(c)
    df["ht_sine"], df["ht_leadsine"] = sine, leadsine
    df["ht_trendmode"] = talib.HT_TRENDMODE(c)

    # ----- Price transforms -------------------------------------------------------
    df["avgprice"] = talib.AVGPRICE(o, h, l, c)
    df["medprice"] = talib.MEDPRICE(h, l)
    df["typprice"] = talib.TYPPRICE(h, l, c)
    df["wclprice"] = talib.WCLPRICE(h, l, c)

    # ----- Volatility -------------------------------------------------------------
    df["atr"] = talib.ATR(h, l, c, timeperiod=14)
    df["natr"] = talib.NATR(h, l, c, timeperiod=14)
    df["trange"] = talib.TRANGE(h, l, c)

    # ----- Statistical / Regression ----------------------------------------------
    df["beta"] = talib.BETA(h, l, timeperiod=tp)
    df["correl"] = talib.CORREL(h, l, timeperiod=tp)
    df["linearreg"] = talib.LINEARREG(c, timeperiod=tp)
    df["linearreg_angle"] = talib.LINEARREG_ANGLE(c, timeperiod=tp)
    df["linearreg_intercept"] = talib.LINEARREG_INTERCEPT(c, timeperiod=tp)
    df["linearreg_slope"] = talib.LINEARREG_SLOPE(c, timeperiod=tp)
    df["stddev"] = talib.STDDEV(c, timeperiod=tp)
    df["tsf"] = talib.TSF(c, timeperiod=tp)
    df["var"] = talib.VAR(c, timeperiod=tp)

    # ----- Candlestick patterns ---------------------------------------------------
    for fn in talib.get_function_groups()["Pattern Recognition"]:
        col = fn.lower()
        df[col] = getattr(talib, fn)(o, h, l, c)  # returns ±100 / 0

    return df.reset_index()


def resample_ohlcv(df, time_horizon):
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
    df.set_index("datetime", inplace=True)

    ohlcv_resampled = (
        df.resample(time_horizon)
        .agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        .dropna()
        .reset_index()
    )

    return ohlcv_resampled


# Add subfolder to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


def _price_arrays(df):
    return (
        df["open"].values,
        df["high"].values,
        df["low"].values,
        df["close"].values,
        df["volume"].values,
    )


def get_last_indicator_timestamp(engine, schema, table):
    query = f"SELECT MAX(datetime) FROM {schema}.{table}"
    try:
        with engine.connect() as conn:
            result = conn.execute(query).scalar()
        return pd.to_datetime(result).tz_localize("UTC") if result else None
    except Exception as e:
        print(f"⚠️ Could not get last indicator timestamp: {e}")
        return None


def resample_ohlcv(df, time_horizon):
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
    df.set_index("datetime", inplace=True)
    return (
        df.resample(time_horizon)
        .agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        .dropna()
        .reset_index()
    )


def main():
    db_params = {
        "dbname": "postgres",
        "user": "postgres",
        "password": "123abc",
        "host": "localhost",
        "port": 5432,
    }

    schema_ohlcv = "bybit_data"
    schema_indicators = "indicators"
    base_table = "btc_usdt_1min"
    resample_timeframe = "1H"
    output_table = "bybit_btc_1h"

    try:
        engine = create_engine(
            f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
        )

        with psycopg2.connect(**db_params) as conn_pg:
            with conn_pg.cursor() as cursor:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_indicators};")
                conn_pg.commit()

            indicator_last_ts = get_last_indicator_timestamp(
                engine, schema_indicators, output_table
            )

            if indicator_last_ts:
                print(f"📌 Last indicator timestamp: {indicator_last_ts}")
                query = f"""
                    SELECT * FROM {schema_ohlcv}.{base_table}
                    WHERE datetime > %s
                    ORDER BY datetime
                """
                df = pd.read_sql_query(query, conn_pg, params=(indicator_last_ts,))
            else:
                print(f"📌 No previous indicators found. Loading full data.")
                df = pd.read_sql_query(
                    f"SELECT * FROM {schema_ohlcv}.{base_table} ORDER BY datetime ASC",
                    conn_pg,
                )

        if df.empty:
            print(f"🚫 No new data found.")
            return

        # Resample data using utility
        df_resampled = resample_ohlcv(df, resample_timeframe)
        if df_resampled.empty:
            print(f"🚫 Resampling failed.")
            return
        print(f"✅ Resampled to {resample_timeframe}, {len(df_resampled)} rows")

        # Generate indicators
        df_indicators = generateIndicators(df_resampled)
        if df_indicators.empty:
            print("🚫 Indicator generation failed.")
            return

        # Drop duplicates based on datetime
        df_indicators["datetime"] = pd.to_datetime(df_indicators["datetime"], utc=True)
        df_indicators.drop_duplicates(subset="datetime", keep="last", inplace=True)

        # Filter only new indicators beyond the last stored timestamp
        if indicator_last_ts:
            df_indicators = df_indicators[df_indicators["datetime"] > indicator_last_ts]

        if df_indicators.empty:
            print("⚠️ No new indicators to save.")
            return

        # Save indicators
        df_indicators.to_sql(
            name=output_table,
            con=engine,
            schema=schema_indicators,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )

        print(
            f"✅ Saved {len(df_indicators)} indicators to {schema_indicators}.{output_table}"
        )

    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
