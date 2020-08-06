# Basic design

# Usage

## Syncing candles

`./sync.py --help`

Sync 1m ETH candles for the last 90 days:
`./sync.py candles --exchange=bitfinex --symbol=tETHUSD`

If you want to sync more data use `--start=2020-01-01` for example. Running this after a previous sync will only sync candles that are missing in the database, at the beginning and end of the range.
