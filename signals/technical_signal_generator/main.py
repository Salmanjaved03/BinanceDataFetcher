import configparser
from datetime import datetime
import os
import sys

# Add subfolder to Python path to allow importing
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from DataDownloader.download_data import DataDownloader
from indicators.get_indicators import generateIndicators
from signals.technical_signal_generator.technical_signals import getSignals

def main():
    config = configparser.ConfigParser()
    config.read(r'signals/technical_signal_generator/config.ini')

    symbols_raw = config['DATA']['symbol']
    symbols = [s.strip() for s in symbols_raw.split(',') if s.strip()]

    exchange = config['DATA']['exchange']
    time_horizon = config['DATA']['time_horizon']
    start_date = config['DATA']['start_date']
    end_date = config['DATA']['end_date']
    
    if 'INDICATORS' not in config:
        raise KeyError("[INDICATORS] section missing in config.ini")

    # convert each key's value to bool (configparser is case‑insensitive)
    section = config['INDICATORS']
    ind_flags = {k.lower(): section.getboolean(k) for k in section}

    # list of indicators whose value is True
    indicators_chosen = [name for name, flag in ind_flags.items() if flag]


    start_date = datetime.strptime(start_date, "%Y-%m-%d")

    if end_date.lower() == 'now':
        end_date = datetime.now()
    else:
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
        
    downloader = DataDownloader(exchange=exchange,symbol=symbols[0], time_horizon=time_horizon, start_date=start_date, end_date=end_date)
    df2 = downloader.fetch_resampled('H')
    df2.to_csv("def.csv")
    indicators_df = generateIndicators(df2)
    indicators_df.to_csv(r"indicators/indicators.csv")
    signals_df = getSignals(indicators_df, indicators_chosen)
    signals_df.to_csv(r"signals/technical_signal_generator/signals.csv")
if __name__ == '__main__':
    main()
