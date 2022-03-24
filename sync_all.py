#!/usr/bin/env python3
""" Quick and dirty script to automate pulling *all symbols for an exchange. """
from candles.sync_candles import get_sync_candles_class
from funding_rates.sync_rates import get_sync_rates_class
from futures.sync_futures import get_sync_futures_class
import arrow
import requests
import sys

END = arrow.utcnow()


def ftx():
    """Sync all perpetual futures' data"""
    for future in requests.get("https://ftx.com/api/futures").json()["result"]:
        if "PERP" not in future["name"]:
            continue
        get_sync_rates_class(exchange="ftx", symbol=future["name"], start="2020-05-13", end=END,).pull_data()
        get_sync_futures_class(exchange="ftx", symbol=future["name"], start="2020-05-13", end=END,).pull_data()


def bitfinex():
    for symbol in requests.get("https://api-pub.bitfinex.com/v2/tickers?symbols=ALL").json():
        cur_symbol = symbol[0]
        if not cur_symbol.startswith("t") or ":" in cur_symbol or "USD" not in cur_symbol:
            continue
        client = get_sync_candles_class(
            exchange="bitfinex", symbol=cur_symbol, interval="1m", start="2018-11-01", end=END
        )
        client.pull_data()


def binance():
    MIN_VOLUME = 1000
    excluded = ["USDCUSDT", "USDSUSDT", "TUSDUSDT", "BUSDUSDT"]
    symbols = requests.get("https://api.binance.com/api/v3/exchangeInfo").json()["symbols"]
    for i, symbol in enumerate(symbols):
        print(f"Processing {i} of {len(symbols)}...")
        cur_symbol = symbol["symbol"]
        margin = symbol["isMarginTradingAllowed"]
        if not margin or "USDT" not in cur_symbol or cur_symbol in excluded:
            continue
        candles = requests.get(
            (
                "https://api.binance.com/api/v3/klines?limit=5&startTime="
                f"{round(arrow.get('2018-11-01').float_timestamp * 1000)}&interval=1d&symbol={cur_symbol}"
            )
        )
        candles.raise_for_status()
        candles = candles.json()
        # we either don't have candles at the start time, or the volume is too low
        if not candles:
            print(f"Skipping {cur_symbol} due to missing candles.")
            continue
        if not float(candles[0][5]) > MIN_VOLUME:
            print(f"Skipping {cur_symbol}, volume is {candles[0][5]} from candle: {candles[0]}")
            continue
        client = get_sync_candles_class(
            exchange="binance", symbol=cur_symbol, interval="1m", start="2018-11-01", end=END
        )
        try:
            client.pull_data()
        except AssertionError as err:
            if "Did not receive" in err.args[0]:
                print(f"Skipping {cur_symbol} as it did not exist at the start time")
                continue
            raise


if "__main__" in __name__:
    locals()[sys.argv[1]]()
