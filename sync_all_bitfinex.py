#!/usr/bin/env python3
from candles.sync_candles import get_sync_candles_class
import requests

for symbol in requests.get("https://api-pub.bitfinex.com/v2/tickers?symbols=ALL").json():
    cur_symbol = symbol[0]
    if not cur_symbol.startswith("t") or ":" in cur_symbol or "USD" not in cur_symbol:
        continue
    client = get_sync_candles_class(exchange="bitfinex", symbol=cur_symbol, interval="1m", start="2018-11-01",)
    client.pull_data()
