import configparser
from datetime import datetime
import os
import sys

# Add subfolder to Python path to allow importing
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from data.binance.data_fetcher import BinanceDataFetcher
from data.bybit.data_fetcher import BybitDataFetcher
from data.download_data import DataDownloader
from indicators.get_indicators import generateIndicators
from signals.technical_signals import getSignals

def main():
    config = configparser.ConfigParser()
    config.read('config.ini')

    symbols_raw = config['DATA']['symbol']
    symbols = [s.strip() for s in symbols_raw.split(',') if s.strip()]

    exchange = config['DATA']['exchange']
    time_horizon = config['DATA']['time_horizon']
    start_date = config['DATA']['start_date']
    end_date = config['DATA']['end_date']

    start_date = datetime.strptime(start_date, "%Y-%m-%d")

    if end_date.lower() == 'now':
        end_date = datetime.now()
    else:
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
    if exchange=="binance":
            fetcher = BinanceDataFetcher(
                exchange=exchange,
                symbol=symbols[0],
                time_horizon=time_horizon,
                start_date=start_date,
                end_date=end_date
            )
    elif exchange=="bybit":
        fetcher = BinanceDataFetcher(
            exchange=exchange,
            symbol=symbols[1],
            time_horizon=time_horizon,
            start_date=start_date,
            end_date=end_date
        )
    downloader = DataDownloader(exchange=exchange,symbol=symbols[0], time_horizon=time_horizon, start_date=start_date, end_date=end_date)
    df2 = downloader.fetch_resampled('H')
    df2.to_csv("def.csv")
    indicators = generateIndicators()
    indicators.to_csv(r"indicators/indicators.csv")
    signals = getSignals(indicators)
    signals.to_csv(r"signals/signals.csv")
if __name__ == '__main__':
    main()
