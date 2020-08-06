# Basic design

Syncs 1m candles in influxdb, and supports querying any interval candle (uses a GROUP BY and does the math for you).

# Usage

## Syncing candles

`./sync.py --help`

Sync ETH candles for the last 90 days:
`./sync.py candles --exchange=bitfinex --symbol=tETHUSD`

If you want to sync more data use `--start=2020-01-01` for example. Running this after a previous sync will only sync candles that are missing in the database, at the beginning and end of the range.
