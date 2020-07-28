#!/usr/bin/env python3
from candles.sync_candles import get_sync_candles_class
import click

COMMANDS = ["candles"]


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
            force_range=options["force_range"],
        )
        client.pull_data()


if __name__ == "__main__":
    run()
