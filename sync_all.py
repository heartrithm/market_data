#!/usr/bin/env python3
""" Quick and dirty script to automate pulling *all symbols for an exchange. """
from candles.sync_candles import get_sync_candles_class
import requests
import sys


def bitfinex():
    for symbol in requests.get("https://api-pub.bitfinex.com/v2/tickers?symbols=ALL").json():
        cur_symbol = symbol[0]
        if not cur_symbol.startswith("t") or ":" in cur_symbol or "USD" not in cur_symbol:
            continue
        client = get_sync_candles_class(exchange="bitfinex", symbol=cur_symbol, interval="1m", start="2018-11-01",)
        client.pull_data()


def binance():
    for symbol in requests.get("https://api.binance.com/api/v3/exchangeInfo").json()["symbols"]:
        cur_symbol = symbol["symbol"]
        if "USDT" not in cur_symbol:
            continue
        client = get_sync_candles_class(exchange="binance", symbol=cur_symbol, interval="1m", start="2018-11-01",)
        client.pull_data()


if "__main__" in __name__:
    locals()[sys.argv[1]]()
