#!/usr/bin/env python3
import os
from loguru import logger

import click

from candles.sync_candles import get_sync_candles_class
from futures.sync_futures import get_sync_futures_class
from tardis import SyncHistorical

COMMANDS = ["candles", "futures", "historical_futures"]
logger.level = os.getenv("LOG_LEVEL", "WARNING")


@click.command()
@click.argument("command", type=click.Choice(COMMANDS))
@click.option("--exchange", type=str, default="Bitfinex", help="Bitfinex, ...")
@click.option(
    "--symbol", type=str, default="fUSD", help="fUSD for Bitfinex funding data, or tETHUSD for ETH",
)
@click.option("--interval", type=str, default="1m", help="1m for 1m candle data")
@click.option("--start", type=str, default=None, help="any string python-arrow supports")
@click.option("--end", type=str, default=None, help="any string python-arrow supports")
def run(*args, **options):  # pragma: no cover
    if options["command"] == "candles":
        exchange = options["exchange"].lower()
        client = get_sync_candles_class(
            exchange=exchange,
            symbol=options["symbol"],
            interval=options["interval"],
            start=options["start"],
            end=options["end"],
        )
        client.pull_data()

    elif options["command"] == "futures":
        exchange = options["exchange"].lower()
        futures_client = get_sync_futures_class(
            exchange=exchange,
            symbol=options["symbol"],
            interval=options["interval"],
            start=options["start"],
            end=options["end"],
            data_type="futures",
        )
        futures_client.pull_data()

        rates_client = get_sync_futures_class(
            exchange=exchange,
            symbol=options["symbol"],
            interval=options["interval"],
            start=options["start"],
            end=options["end"],
            data_type="funding_rates",
        )
        rates_client.pull_data()

    elif options["command"] == "historical_futures":
        exchange = options["exchange"].lower()
        tardis = SyncHistorical(exchange, options["symbol"], interval="1h", start=options["start"], end=options["end"])
        tardis.sync()

if __name__ == "__main__":
    run()
