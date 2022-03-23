import sys

import arrow
from ratelimit import limits, sleep_and_retry

from candles.sync_candles import BaseSyncCandles
from exchanges.apis.ftx import FTXApi
from tardis import SyncHistorical


IS_PYTEST = "pytest" in sys.modules


def get_sync_futures_class(exchange, symbol, interval=None, start=None, end=None, host=None):
    """ Abstraction layer that returns an instance of the right SyncCandles class """
    if exchange.lower() == "ftx":
        # FTX only supports 1h
        return SyncFTXFutures(symbol, "1h", start, end, host, data_type="futures")


class SyncFTXFutures(BaseSyncCandles):
    """ Sync futures data for FTX
        Some comes from FTX, some from tardis.dev (historical data).
    """

    DEFAULT_SYNC_DAYS = 1
    API_MAX_RECORDS = None
    API_CALLS_PER_MIN = 100000 if IS_PYTEST else 1000
    EXCHANGE = "ftx"

    def api_client(self):
        if not self.client:
            # Cache/reuse the client object so that sessions are re-used, which enables HTTP Keep-Alive
            self.client = FTXApi()
        return self.client

    @sleep_and_retry
    @limits(calls=API_CALLS_PER_MIN, period=60)  # calls per minute
    def call_api(self, endpoint, params):
        # FTX futures calls return 1 item
        res = self.client.brequest(1, endpoint=endpoint, params=params)
        if "nextFundingTime" in res["result"]:
            res["result"]["time"] = arrow.get(res["result"]["nextFundingTime"]).timestamp()
        return res  # one item returned from FTX /futures/ API - add list because we iterate later

    def pull_data(self):
        # call tardis to get history...
        SyncHistorical(self.symbol, self.interval, start=self.start, end=self.end).pull_data()

        # ...then ftx to get current snapshot
        self.candle_order = None
        endpoint = [f"futures/{self.symbol}", f"futures/{self.symbol}/stats"]

        self.sync(
            endpoint,
            extra_tags={"source": "ftx"},
            start_format="start_time",
            end_format="end_time",
            timestamp_units="s",
            result_key="result",
            merge_endpoint_results_dict=True,
        )
