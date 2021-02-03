[![Coverage Status](https://coveralls.io/repos/github/heartrithm/market_data/badge.svg?branch=master)](https://coveralls.io/github/heartrithm/market_data?branch=master)

# Basic design

# Usage

## Syncing candles

`./sync.py --help`

Sync 1m ETH candles for the last 90 days:
`./sync.py candles --exchange=bitfinex --symbol=tETHUSD`

If you want to sync more data use `--start=2020-01-01` for example. All syncs where candles are missing at the beginning of the range will catch up from start to [first candle in db], and then also sync [last candle in db] to now() (if --end isn't specified).
