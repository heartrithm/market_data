from exchanges.apis.ftx import FTXApi

from ratelimit import limits, sleep_and_retry
from candles.sync_candles import BaseSyncCandles

import sys

IS_PYTEST = "pytest" in sys.modules


def get_sync_rates_class(exchange, symbol, interval=None, start=None, end=None, host=None):
    """ Abstraction layer that returns an instance of the right SyncRates class """
    if exchange.lower() == "ftx":
        # FTX only supports 1h
        return SyncFTXRates(symbol, "1h", start, end, host, data_type="funding_rates")


class SyncFTXRates(BaseSyncCandles):
    """ Sync funding rates for FTX """

    DEFAULT_SYNC_DAYS = 90
    API_MAX_RECORDS = 500
    API_CALLS_PER_MIN = 30
    EXCHANGE = "ftx"

    def api_client(self):
        if not self.client:
            # Cache/reuse the client object so that sessions are re-used, which enables HTTP Keep-Alive
            self.client = FTXApi()
        return self.client

    @sleep_and_retry
    @limits(calls=API_CALLS_PER_MIN, period=60)  # calls per minute
    def call_api(self, endpoint, params):
        res = self.client.brequest(1, endpoint=endpoint, params=params)
        return res

    def pull_data(self):
        self.candle_order = None
        endpoint = "funding_rates"

        self.sync(
            endpoint,
            extra_tags={"source": "ftx"},
            extra_params={"future": self.symbol},
            start_format="start_time",
            end_format="end_time",
            timestamp_units="s",
            result_key="result",
        )
