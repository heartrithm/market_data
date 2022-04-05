import arrow
import asyncio

from loguru import logger
from tardis_client import TardisClient, Channel

from candles.sync_candles import BaseSyncCandles
from utils import get_aws_secret


class SyncHistorical(BaseSyncCandles):
    """Class to enable syncing historical data via Tardis - does not use candles.sync_candles because
    the tardis API is so weird/broken, it's difficult to fit into the model. We still inherit from BaseSyncCandles
    so that write_candles() uses the same logic as when we're sync'ing from FTX directly, i.e. it parses all
    data and decides whether it's a tag or a field.
    """
    EXCHANGE = "ftx"

    def __init__(self, exchange, symbol, interval, start=None, end=None, data_type="futures"):
        self.exchange = exchange
        self.symbol = symbol
        self.interval = interval
        self.start = start
        self.end = end
        self.data_type = data_type

        self.data = []
        super().__init__(self.symbol, self.interval, start=self.start, end=self.end, data_type=self.data_type)

    def api_client(self):
        return TardisClient(api_key=get_aws_secret("tardis.dev")["api_key"])

    def sync(self):
        """Run the sync"""
        asyncio.run(self.replay())
        logger.debug(f"Found {len(self.data)} dates to sync: {[x['nextFundingTime'] for x in self.data]}")
        self.write_candles(self.data, timestamp_units="s")

    async def replay(self):
        # replay method returns Async Generator
        messages = self.client.replay(
            exchange="ftx",
            from_date=arrow.get(self.start).format("YYYY-MM-DD"),
            to_date=arrow.get(self.end).format("YYYY-MM-DD"),
            filters=[Channel(name="instrument", symbols=[self.symbol])],
        )

        # unpack messages provided by FTX real-time stream:
        async for local_timestamp, message in messages:
            # fields in message defined here:
            #
            #    https://docs.ftx.com/#get-future
            #    https://docs.ftx.com/#get-future-stats
            #
            # also, there is a collision on "openInterest" and when we
            # join these two dictionaries one gets obliterated so we will
            # keep the one that comes with "stats" since that is where
            # FTX documents it

            # do we have this timestamp already?
            timestamp = arrow.get(message["data"]["stats"]["nextFundingTime"]).timestamp * 1000  # ms
            found = [timestamp for x in self.data if x.get("time", 0) == timestamp]

            if not found:
                # new data point!
                time = {"time": timestamp}
                self.data.append(time | message["data"]["info"] | message["data"]["stats"])
