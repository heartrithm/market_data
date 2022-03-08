#!/usr/bin/env python3
from candles.sync_candles import get_sync_candles_class
from futures.sync_futures import get_sync_futures_class
from loguru import logger
import click
import os

COMMANDS = ["candles", "futures"]
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
        client = get_sync_futures_class(
            exchange=exchange,
            symbol=options["symbol"],
            interval=options["interval"],
            start=options["start"],
            end=options["end"],
        )
        client.pull_data()


if __name__ == "__main__":
    run()
