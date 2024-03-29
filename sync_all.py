#!/usr/bin/env python3
""" Quick and dirty script to automate pulling *all symbols for an exchange. """
import sys

import arrow
import requests

from candles.sync_candles import get_sync_candles_class
from tardis import SyncHistorical

END = arrow.utcnow()


def ftx(days_ago=14, host=None):
    """FTX candle data"""

    excluded_symbols = ["BULL", "BEAR", "HEDGE", "HALF", "MOVE"]

    def _is_excluded(symbol):
        # filter out futures like BTC-0624, and anything in excluded_symbols
        if [x for x in excluded_symbols if x in symbol] or any(char.isdigit() for char in symbol.split("-")[-1]):
            return True
        return False

    for symbol in requests.get("https://ftx.com/api/markets").json()["result"]:
        symbol = symbol["name"]
        if not _is_excluded(symbol):
            client = get_sync_candles_class(
                exchange="ftx",
                symbol=symbol,
                interval="1m",
                host=host,
                start=arrow.utcnow().shift(days=-int(days_ago)),
                end=END,
            )
            client.pull_data()


def ftx_futures(days_ago=8):
    """Comes from tardis data, as FTX doesn't provide history"""

    symbols = []
    for future in requests.get("https://ftx.com/api/futures").json()["result"]:
        if "PERP" in future["name"]:
            symbols.append(future["name"])

    exchange = "ftx"
    if days_ago:
        start = END.shift(days=-int(days_ago)).format("YYYY-MM-DD")
    tardis = SyncHistorical(
        exchange,
        "BTC-PERP",
        interval="1h",
        start=start,
        end=None,
        symbols=symbols,
    )
    tardis.sync()


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
    locals()[sys.argv[1]](*sys.argv[2:])
